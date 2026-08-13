# Persistent Writeback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in RAM-to-local-SSD-to-remote writeback object-store overlay that gives ZeroFS locally durable `fsync`, explicit remote barriers, crash recovery, and read-your-writes semantics over the existing single-writer SFTP backend.

**Architecture:** Wrap the retrying remote store in a `WritebackObjectStore`. Accepted mutations receive contiguous sequence numbers, live in a bounded RAM admission tier, become locally durable as redb records plus fsynced external blobs, and replay to the remote store through a dependency-aware scheduler. A typed `WritebackLifecycle` exposes local and remote barriers to the filesystem, RPC, CLI, shutdown path, metrics, and recovery tests.

**Tech Stack:** Rust 2024, Tokio 1.49, `object_store` 0.14, redb 4.1.0, SHA-256, UUID, tonic/prost, Prometheus metrics, existing ZeroFS fault/failpoint harnesses.

## Global Constraints

- The feature is disabled by default; enabling it without `ack_mode` defaults to `memory` (client flush barriers still force SSD durability).
- The CT105 pilot explicitly uses `ack_mode = "memory"`, 16 GB dirty RAM, 512 GB dirty SSD, and a 256 GB filesystem free-space reserve.
- `[writeback].memory_size_gb` is an additional dirty-write budget, independent of `[cache].memory_size_gb`; writeback admission must never consume, resize, evict, or borrow capacity from the clean Foyer read cache.
- A completed filesystem `fsync` or `sync` must be covered by `local_seq`; it must never acknowledge RAM-only state.
- `zerofs flush --remote` must wait for `remote_seq` to cover the barrier captured after the filesystem seal and metadata flush.
- Exactly one read-write daemon may own a writeback journal and SFTP prefix; HA read-write mode is rejected.
- Existing SFTP limits remain authoritative: eight shared sessions and no more than seven read or seven write operations.
- Only recognized immutable creates may publish out of sequence; overwrites, conditional updates, deletes, copies, renames, unknown paths, and explicit barriers are fences.
- No mutation coalescing is allowed in the first implementation.
- Losing the local SSD can lose locally durable mutations not yet covered by `remote_seq`; the CLI and status output must state that boundary.
- Journal corruption, identity mismatch, reserve violation, or remote divergence fails closed instead of skipping data.
- Production changes follow strict RED/GREEN cycles and each independently useful slice is committed before the next slice.

---

## File Structure

- `zerofs/src/writeback/mod.rs`: public constructor, `WritebackObjectStore`, `WritebackLifecycle`, module exports, and object-store delegation.
- `zerofs/src/writeback/config.rs`: normalized byte budgets and cross-backend validation derived from top-level TOML settings.
- `zerofs/src/writeback/model.rs`: serialized mutation, identity, watermark, local ETag, operation, fence, and status types.
- `zerofs/src/writeback/journal.rs`: exclusive lock, directory safety, redb schema, blob commit, recovery validation, durable watermarks, and cleanup transactions.
- `zerofs/src/writeback/admission.rs`: exact RAM/SSD capacity reservations, cancellation-safe admission, local journaler queue, and backpressure.
- `zerofs/src/writeback/overlay.rs`: newest-key index plus GET, HEAD, ranged GET, LIST, create/update preconditions, tombstones, copy, and rename resolution.
- `zerofs/src/writeback/multipart.rs`: private multipart staging whose completion admits one atomic put and whose abort frees all local capacity.
- `zerofs/src/writeback/scheduler.rs`: immutable/fence classification, per-key FIFO, remote workers, contiguous remote watermark, retries, lost-reply reconciliation, and divergence poisoning.
- `zerofs/src/writeback/metrics.rs`: atomic status snapshot and Prometheus metric emission.
- `zerofs/src/config.rs`: TOML `WritebackConfig`, enums, safe defaults, rendering, and cross-section validation.
- `zerofs/src/cli/init.rs`: bucket-scoped identity construction and writeback store placement in the object-store stack.
- `zerofs/src/fs/flush_coordinator.rs`: post-database-flush local barrier hook.
- `zerofs/src/cli/server.rs`: retain lifecycle, expose it to RPC, and stop it before the SFTP pool.
- `zerofs/src/rpc/server.rs`, `zerofs/src/rpc/client.rs`, `zerofs/proto/admin.proto`: typed local/remote flush and writeback status API.
- `zerofs/src/cli/flush.rs`, `zerofs/src/cli/writeback.rs`, `zerofs/src/cli/mod.rs`, `zerofs/src/main.rs`: operator commands.
- `zerofs/src/prometheus.rs`: export writeback watermarks, bytes, age, rates, retries, errors, and backpressure.
- `zerofs/tests/writeback_recovery.rs`: process-restart and crash-prefix integration tests.
- `zerofs/tests/writeback_faults.rs`: filesystem/journal/remote fault tests.

---

### Task 1: Configuration, durable model, and dependency boundary

**Files:**
- Modify: `zerofs/Cargo.toml`
- Modify: `zerofs/src/main.rs`
- Modify: `zerofs/src/config.rs`
- Create: `zerofs/src/writeback/mod.rs`
- Create: `zerofs/src/writeback/config.rs`
- Create: `zerofs/src/writeback/model.rs`

**Interfaces:**
- Produces: `AckMode`, `ShutdownFlush`, library-owned `WritebackAccessMode`, top-level `WritebackConfig`, normalized `WritebackSettings`, `JournalIdentity`, `MutationRecord`, `MutationKind`, `FenceClass`, `WritebackStatus`, and `LocalEtag`.
- Consumes: `Settings::sftp_endpoint()`, `SftpConfig::write_concurrency`, and the resolved bucket identity from startup. The binary maps its private `DatabaseMode` to `WritebackAccessMode` at the startup seam.

- [ ] **Step 1: Write failing configuration tests**

Add literal TOML tests in `config.rs` for disabled default, implicit `ssd`, explicit `memory`, independent clean-read and dirty-write memory budgets, zero budgets, watermark ordering, upload concurrency above SFTP concurrency, HA conflict, read-only/checkpoint conflict through `WritebackSettings::from_settings`, and a path nested below the clean cache directory.

```rust
#[test]
fn writeback_memory_profile_normalizes_exact_byte_budgets() {
    let settings = settings_from_toml(r#"
[writeback]
enabled = true
dir = "/var/cache/zerofs/writeback"
ack_mode = "memory"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
high_watermark_percent = 95
resume_percent = 85
upload_concurrency = 7
shutdown_flush = "local"
"#);

    let normalized = settings.writeback_settings(WritebackAccessMode::ReadWrite).unwrap().unwrap();
    assert_eq!(normalized.memory_bytes, 16_000_000_000);
    assert_eq!(normalized.disk_bytes, 512_000_000_000);
    assert_eq!(normalized.min_free_bytes, 256_000_000_000);
    assert_eq!(normalized.ack_mode, AckMode::Memory);
}

#[test]
fn dirty_memory_budget_is_additional_to_clean_read_cache() {
    let settings = settings_with_clean_cache_gb_and_dirty_writeback_gb(11.0, 16.0);
    let normalized = settings.writeback_settings(WritebackAccessMode::ReadWrite).unwrap().unwrap();
    assert_eq!(settings.cache.memory_size_gb, 11.0);
    assert_eq!(normalized.memory_bytes, 16_000_000_000);
}
```

- [ ] **Step 2: Run the focused tests and record RED**

Run: `cargo test --lib config::tests::writeback -- --nocapture`

Expected: compile failure because `WritebackConfig`, `AckMode`, and `writeback_settings` do not exist.

- [ ] **Step 3: Add the model and validated configuration**

Add `redb = "4.1.0"` and expose `mod writeback;`. Implement these stable signatures:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckMode { Remote, Ssd, Memory }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShutdownFlush { Local, Remote }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritebackConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub ack_mode: AckMode,
    pub memory_size_gb: f64,
    pub disk_size_gb: f64,
    pub min_free_gb: f64,
    pub high_watermark_percent: u8,
    pub resume_percent: u8,
    pub upload_concurrency: usize,
    pub shutdown_flush: ShutdownFlush,
}

pub struct WritebackSettings {
    pub dir: PathBuf,
    pub ack_mode: AckMode,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub min_free_bytes: u64,
    pub high_watermark_percent: u8,
    pub resume_percent: u8,
    pub upload_concurrency: usize,
    pub shutdown_flush: ShutdownFlush,
}
```

Use decimal GB consistently because the approved config is expressed in GB. Reject NaN, infinity, negative numbers, multiplication overflow, nested clean-cache paths after lexical normalization, symlinks once the path exists, HA read-write mode, and read-only/checkpoint mode.

- [ ] **Step 4: Run focused configuration tests and record GREEN**

Run: `cargo test --lib config::tests::writeback -- --nocapture`

Expected: all writeback configuration tests pass.

- [ ] **Step 5: Commit the slice**

```bash
git add zerofs/Cargo.toml zerofs/Cargo.lock zerofs/src/main.rs zerofs/src/config.rs zerofs/src/writeback
git commit -m "feat(writeback): define durable journal configuration"
```

---

### Task 2: Exclusive crash-safe SSD journal

**Files:**
- Create: `zerofs/src/writeback/journal.rs`
- Modify: `zerofs/src/writeback/model.rs`
- Modify: `zerofs/src/writeback/mod.rs`
- Modify: `zerofs/Cargo.toml`

**Interfaces:**
- Consumes: `WritebackSettings`, `JournalIdentity`, `MutationRecord`, `MutationKind`.
- Produces: `Journal::open`, `Journal::commit_put`, `Journal::commit_metadata`, `Journal::mark_remote`, `Journal::recover`, `Journal::remove_remote_prefix`, and `JournalSnapshot`.

- [ ] **Step 1: Write failing real-filesystem journal tests**

Use `tempfile::TempDir` and real redb/filesystem operations. Each test names the mutation that would break it: missing file fsync, missing blob-directory fsync, record committed before blob publication, sequence reuse, wrong journal identity, missing blob, corrupt blob, symlink traversal, wrong mode, and a second process lock owner.

```rust
#[test]
fn committed_put_reopens_with_verified_blob_and_contiguous_local_watermark() {
    let root = tempfile::tempdir().unwrap();
    let identity = literal_identity("bucket-a", "prefix-a", "key-a");
    let mut journal = Journal::open(root.path(), identity.clone()).unwrap();
    let record = literal_put(1, "segments/1", b"payload");

    journal.commit_put(&record, b"payload").unwrap();
    drop(journal);

    let reopened = Journal::open(root.path(), identity).unwrap();
    assert_eq!(reopened.snapshot().local_seq, 1);
    assert_eq!(reopened.read_blob(1).unwrap(), b"payload");
}
```

- [ ] **Step 2: Run journal tests and record RED**

Run: `cargo test --lib writeback::journal::tests -- --nocapture`

Expected: compile failure because `Journal` and its schema are absent.

- [ ] **Step 3: Implement the journal schema and file protocol**

Use a process-held lock file with an OS advisory exclusive lock; add a small direct dependency that supports Unix file locks if the standard library in the pinned toolchain does not. Define redb tables with stable byte serialization:

```rust
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const MUTATIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("mutations");
const KEY_HEADS: TableDefinition<&str, u64> = TableDefinition::new("key_heads");
```

The durable put order is: unique `tmp` file mode 0600, write, length/hash verification, `sync_all`, atomic rename into a 0700 blob shard, directory fsync, then one durable redb write transaction storing the record and advancing `local_seq`. Metadata-only records use the same transaction rule without a blob. Sequence `N + 1` is rejected unless `local_seq == N`.

- [ ] **Step 4: Implement recovery validation and cleanup**

On open, reject symlink components, wrong owner, wrong mode, format or identity mismatch, sequence gaps, `remote_seq > local_seq`, absent blobs, and mismatched length/hash. Remove only files in `tmp/`; preserve unreferenced blob files as an explicit corruption error until an operator repair command exists.

- [ ] **Step 5: Run journal tests, full library tests, and record GREEN**

Run:

```bash
cargo test --lib writeback::journal::tests -- --nocapture
cargo test --lib
```

Expected: focused journal tests pass and the library suite has zero failures.

- [ ] **Step 6: Commit the slice**

```bash
git add zerofs/Cargo.toml zerofs/Cargo.lock zerofs/src/writeback
git commit -m "feat(writeback): persist crash-safe mutation journal"
```

---

### Task 3: Bounded RAM admission and local journaler

**Files:**
- Create: `zerofs/src/writeback/admission.rs`
- Modify: `zerofs/src/writeback/model.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Consumes: `Journal`, `WritebackSettings`, `MutationRecord`.
- Produces: `Admission::reserve`, `AdmissionPermit::accept`, `LocalJournaler`, `LocalBarrier`, and cancellation-safe `WritebackLifecycle::wait_local`.

- [ ] **Step 1: Write failing bounded-admission tests**

Test real concurrent futures with literal capacities: exact fit, one-byte overflow, two concurrent large puts, cancellation while blocked, journal error poisoning, RAM release only after local commit, SSD high-water pause, free-space reserve pause, resume watermark hysteresis, and shutdown waking all waiters.

```rust
#[tokio::test]
async fn concurrent_puts_cannot_oversubscribe_dirty_ram() {
    let admission = Admission::new(10);
    let first = admission.reserve(7).await.unwrap();
    let second = tokio::spawn({
        let admission = admission.clone();
        async move { admission.reserve(4).await }
    });
    assert!(tokio::time::timeout(Duration::from_millis(25), second).await.is_err());
    drop(first);
    assert_eq!(admission.reserve(4).await.unwrap().bytes(), 4);
}
```

- [ ] **Step 2: Run admission tests and record RED**

Run: `cargo test --lib writeback::admission::tests -- --nocapture`

Expected: compile failure because bounded admission types do not exist.

- [ ] **Step 3: Implement capacity reservation and local worker**

Represent RAM capacity with a fair FIFO waiter queue and `u64` accounting rather than `Semaphore` permits, because object payloads can exceed `u32` permit counts. Reserve before collecting `PutPayload` into a contiguous owned buffer. Transfer ownership from `AdmissionPermit` to the mutation only after sequence allocation and overlay insertion are ready to commit atomically.

The journaler persists sequences strictly in order and advances `local_seq` only as a contiguous prefix. Memory-mode callers return after acceptance; SSD-mode callers await `local_seq`; remote-mode callers await `remote_seq`. A failed local write poisons admission and returns a typed `LocalDurability` object-store error to new writers.

- [ ] **Step 4: Run focused and library tests and record GREEN**

Run:

```bash
cargo test --lib writeback::admission::tests -- --nocapture
cargo test --lib
```

- [ ] **Step 5: Commit the slice**

```bash
git add zerofs/src/writeback
git commit -m "feat(writeback): add bounded RAM to SSD admission"
```

---

### Task 4: Read-your-writes namespace overlay

**Files:**
- Create: `zerofs/src/writeback/overlay.rs`
- Modify: `zerofs/src/writeback/model.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Consumes: recovered journal snapshot, RAM payload owners, remote `Arc<dyn ObjectStore>`.
- Produces: overlay-visible `get_opts`, `head`, `list`, `list_with_delimiter`, create/update precondition checks, and locally namespaced ETags.

- [ ] **Step 1: Write failing model and property tests**

Build a small literal reference namespace and generated operation streams. Assert full/ranged GET, suffix range, HEAD, conditional GET, LIST, delimiter LIST, pending put precedence, pending delete tombstones, create collision, stale update, local ETag matching, and cleanup after `remote_seq` without a visibility gap.

```rust
#[tokio::test]
async fn pending_delete_hides_remote_object_from_get_head_and_list() {
    let remote = in_memory_store_with("tree/a", b"remote").await;
    let overlay = recovered_overlay(remote, vec![delete_record(1, "tree/a")]).await;

    assert_not_found(overlay.get(&Path::from("tree/a")).await);
    assert_not_found(overlay.head(&Path::from("tree/a")).await);
    assert_eq!(collect_paths(overlay.list(Some(&Path::from("tree")))).await, Vec::<String>::new());
}
```

- [ ] **Step 2: Run overlay tests and record RED**

Run: `cargo test --lib writeback::overlay::tests -- --nocapture`

Expected: compile failure because `OverlayIndex` is absent.

- [ ] **Step 3: Implement newest-key index and reads**

Use `BTreeMap<ObjectPath, VecDeque<Sequence>>` behind a Tokio `RwLock`; the final entry is visible. Payload ownership is an enum of bounded RAM bytes or a verified journal blob locator. Produce `GetResult` with exact range and precondition behavior matching `object_store` 0.14. Merge remote listing into a keyed `BTreeMap`, then apply pending visible entries and tombstones deterministically.

- [ ] **Step 4: Run property tests with a fixed seed and record GREEN**

Run:

```bash
PROPTEST_CASES=512 cargo test --lib writeback::overlay::tests -- --nocapture
cargo test --lib
```

- [ ] **Step 5: Commit the slice**

```bash
git add zerofs/src/writeback
git commit -m "feat(writeback): overlay pending namespace mutations"
```

---

### Task 5: Complete ObjectStore mutations and multipart behavior

**Files:**
- Create: `zerofs/src/writeback/multipart.rs`
- Modify: `zerofs/src/writeback/mod.rs`
- Modify: `zerofs/src/writeback/overlay.rs`
- Modify: `zerofs/src/writeback/model.rs`

**Interfaces:**
- Consumes: admission, overlay, journaler, `PutOptions`, `CopyOptions`, `RenameOptions`, and `MultipartUpload`.
- Produces: a complete `ObjectStore` implementation and one-mutation multipart completion.

- [ ] **Step 1: Write failing mutation tests**

Exercise real `ObjectStoreExt` calls against an in-memory remote: overwrite, create, local-version update, remote-version update, stale update, delete stream ordering, copy source resolved from overlay, rename source resolved from overlay, incomplete multipart invisibility, out-of-order parts, completion as one put, abort freeing RAM/tmp bytes, and dropped upload cleanup.

- [ ] **Step 2: Run mutation tests and record RED**

Run: `cargo test --lib writeback::tests::object_store_mutations -- --nocapture`

Expected: missing `ObjectStore` methods or `NotSupported` from the wrapper.

- [ ] **Step 3: Implement mutation admission under per-key locks**

Use a sharded per-key lock table. For two-key copy/rename, acquire locks in lexical path order to avoid deadlock. Validate `PutMode` against the overlay-visible version while holding the key lock, allocate a sequence, install the overlay effect, enqueue the journal operation, then release the lock. A cancellation before installation leaves no sequence, payload, file, or capacity charge; after installation the operation is owned by writeback and completes according to the configured acknowledgement mode.

- [ ] **Step 4: Implement private multipart staging**

Store parts under `writeback/tmp/multipart/<upload-uuid>/` with mode 0700 and part files mode 0600. `complete()` validates the ordered part set, streams it once into the normal put admission path, and only then removes the multipart directory. `abort()` and `Drop` remove private parts and release reserved local bytes. Multipart parts do not consume mutation sequences.

- [ ] **Step 5: Run focused and full library tests and record GREEN**

Run:

```bash
cargo test --lib writeback::tests::object_store_mutations -- --nocapture
cargo test --lib
```

- [ ] **Step 6: Commit the slice**

```bash
git add zerofs/src/writeback
git commit -m "feat(writeback): implement atomic namespace mutations"
```

---

### Task 6: Ordered remote scheduler and divergence handling

**Files:**
- Create: `zerofs/src/writeback/scheduler.rs`
- Modify: `zerofs/src/writeback/journal.rs`
- Modify: `zerofs/src/writeback/model.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Consumes: locally durable mutation stream, remote object store, expected predecessor metadata, `upload_concurrency`.
- Produces: contiguous `remote_seq`, `RemoteBarrier`, exact-content lost-reply reconciliation, poison status, and SSD cleanup.

- [ ] **Step 1: Write failing scheduler tests with a real controllable store**

Implement a test-only store that blocks actual object-store futures but preserves real payload, ETag, and namespace behavior. Assert seven immutable creates overlap; fences wait for all prior sequences; later work waits behind a fence; same-key FIFO; remote watermark does not jump gaps; unexpected predecessor poisons; lost reply plus exact bytes reconciles; mismatched bytes poison; remote outage retries with bounded backoff; cancellation/shutdown does not lose owned work.

```rust
#[tokio::test]
async fn manifest_fence_never_publishes_before_prior_segments() {
    let remote = GatedStore::new();
    let scheduler = scheduler(remote.clone(), 7);
    scheduler.enqueue(immutable_create(1, "segments/a", b"a")).await.unwrap();
    scheduler.enqueue(immutable_create(2, "segments/b", b"b")).await.unwrap();
    scheduler.enqueue(fenced_update(3, "manifest", "etag-0", b"m1")).await.unwrap();

    remote.wait_until_started(["segments/a", "segments/b"]).await;
    assert!(!remote.started("manifest"));
    remote.release("segments/a");
    remote.release("segments/b");
    remote.wait_until_started(["manifest"]).await;
}
```

- [ ] **Step 2: Run scheduler tests and record RED**

Run: `cargo test --lib writeback::scheduler::tests -- --nocapture`

Expected: compile failure because `RemoteScheduler` is absent.

- [ ] **Step 3: Implement fail-safe classification and worker ownership**

Recognize only `PutMode::Create` under the exact segment and SlateDB immutable path grammars already emitted by this revision. Everything else is `Fence`. The dispatcher tracks the earliest incomplete sequence, a set of active immutable sequences, and per-key queues. It starts up to `upload_concurrency` immutable creates before the next fence; a fence starts only when every earlier sequence is complete and blocks every later sequence until applied.

- [ ] **Step 4: Implement publication, reconciliation, and cleanup**

Translate local predecessor ETags to recorded remote ETags. On a lost reply, HEAD the target and fetch/hash bytes only when metadata indicates possible success. Exact content marks success; mismatch or unexpected remote version poisons the lifecycle. After a contiguous remote advancement, durably store the remote result and watermark before removing the blob and journal record. Directory fsync follows blob deletion.

- [ ] **Step 5: Run scheduler and library tests and record GREEN**

Run:

```bash
cargo test --lib writeback::scheduler::tests -- --nocapture
cargo test --lib
```

- [ ] **Step 6: Commit the slice**

```bash
git add zerofs/src/writeback
git commit -m "feat(writeback): drain mutations with remote fences"
```

---

### Task 7: Startup stack, filesystem barriers, and bounded shutdown

**Files:**
- Modify: `zerofs/src/cli/init.rs`
- Modify: `zerofs/src/fs/flush_coordinator.rs`
- Modify: `zerofs/src/cli/server.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Consumes: resolved bucket ID, normalized prefix/backend/key identity, retrying remote store, filesystem flush coordinator, SFTP lifecycle.
- Produces: initialized `WritebackLifecycle`, post-flush local barrier, final local/remote shutdown barrier, and recovery before serving.

- [ ] **Step 1: Write failing stack-order and barrier tests**

Add constructor tests proving compatibility and encryption-key load use the raw remote store before writeback opens; the serving store is `WritebackObjectStore -> RetryingObjectStore -> TracingObjectStore -> SFTP`; recovered overlay exists before SlateDB opens; memory-mode filesystem flush waits for local persistence; remote flush waits for remote publication; final shutdown stops writeback before SFTP pool shutdown; read-only/checkpoint startup rejects enabled writeback.

- [ ] **Step 2: Run stack tests and record RED**

Run:

```bash
cargo test --lib cli::init::tests::writeback -- --nocapture
cargo test --bin zerofs cli::server::tests::writeback -- --nocapture
```

- [ ] **Step 3: Wire startup identity and store stack**

Construct `JournalIdentity` from format version, bucket UUID, normalized object-store URL without credentials, exact database prefix, backend type, and SHA-256 of the encryption-key identity. Open and recover writeback only after bucket compatibility and key loading, but before SlateDB open. Retain the lifecycle in `StartupContext`, `InitResult`, and the serving runtime.

- [ ] **Step 4: Add post-flush local barrier hook**

Extend `FlushCoordinator` with a first-set post-flush hook:

```rust
type PostFlushHook = Arc<dyn Fn() -> BoxFuture<'static, Result<(), FsError>> + Send + Sync>;

pub fn set_local_durability_barrier(&self, hook: PostFlushHook);
```

The worker still seals segments first and flushes SlateDB second; only after both succeed does it capture the current accepted sequence and wait for `local_seq`. This hook runs for ordinary flush, fsync, periodic flush, checkpoint pre-flush, and close.

- [ ] **Step 5: Implement bounded lifecycle shutdown**

Stop admission, run the final filesystem close barrier, wait for the configured local or remote watermark, cancel dispatcher/journaler tasks, join them with the existing bounded shutdown policy, close redb and release the journal lock, then shut down SFTP. A deadline failure leaves the journal intact and returns an error; it never deletes pending records.

- [ ] **Step 6: Run focused, library, and binary tests and record GREEN**

Run:

```bash
cargo test --lib cli::init::tests::writeback -- --nocapture
cargo test --bin zerofs cli::server::tests::writeback -- --nocapture
cargo test --lib
cargo test --bin zerofs
```

- [ ] **Step 7: Commit the slice**

```bash
git add zerofs/src/cli/init.rs zerofs/src/cli/server.rs zerofs/src/fs/flush_coordinator.rs zerofs/src/writeback
git commit -m "feat(writeback): bind local durability to filesystem flush"
```

---

### Task 8: RPC, CLI, status, and Prometheus operations

**Files:**
- Modify: `zerofs/proto/admin.proto`
- Modify: `zerofs/src/rpc/server.rs`
- Modify: `zerofs/src/rpc/client.rs`
- Modify: `zerofs/src/cli/flush.rs`
- Create: `zerofs/src/cli/writeback.rs`
- Modify: `zerofs/src/cli/mod.rs`
- Modify: `zerofs/src/main.rs`
- Create: `zerofs/src/writeback/metrics.rs`
- Modify: `zerofs/src/prometheus.rs`

**Interfaces:**
- Consumes: `WritebackLifecycle::barrier`, `WritebackLifecycle::status`.
- Produces: `FlushMode::{Configured,Local,Remote}`, `WritebackStatusResponse`, CLI status output, and metrics.

- [ ] **Step 1: Write failing CLI/RPC behavior tests**

Test clap conflicts (`--local` and `--remote`), configured-mode default, explicit local and remote requests, disabled-writeback status, status field conversion, RPC error mapping after poison, and exact numeric metric snapshots. Assert behavior through parsed commands and a real in-process tonic service rather than source text.

- [ ] **Step 2: Run CLI/RPC tests and record RED**

Run:

```bash
cargo test --bin zerofs cli::tests::writeback -- --nocapture
cargo test --lib rpc::server::tests::writeback -- --nocapture
```

- [ ] **Step 3: Extend the protobuf and operator commands**

Replace the empty flush request with a backward-compatible enum field whose zero value means configured behavior. Add a status RPC with accepted/local/remote sequences; dirty RAM/SSD bytes and ops; oldest age; remote bytes/ops; retries; terminal error; worker activity; last success; and cumulative backpressure nanoseconds.

CLI behavior:

```text
zerofs flush -c FILE              # configured acknowledgement mode
zerofs flush --local -c FILE      # local SSD barrier
zerofs flush --remote -c FILE     # Storage Box barrier
zerofs writeback status -c FILE   # watermarks/capacity/error boundary
```

- [ ] **Step 4: Export status through Prometheus**

Register gauges/counters using the approved metric names prefixed `zerofs_writeback_`. Emit exact sequence values as gauges only while they remain exactly representable as `f64`; also export low/high 32-bit gauges so long-running instances retain exact observability.

- [ ] **Step 5: Run CLI/RPC/prometheus tests and record GREEN**

Run:

```bash
cargo test --bin zerofs cli::tests::writeback -- --nocapture
cargo test --lib rpc::server::tests::writeback -- --nocapture
cargo test --lib prometheus::tests -- --nocapture
cargo test --lib
cargo test --bin zerofs
```

- [ ] **Step 6: Commit the slice**

```bash
git add zerofs/proto/admin.proto zerofs/src/cli zerofs/src/main.rs zerofs/src/rpc zerofs/src/prometheus.rs zerofs/src/writeback
git commit -m "feat(writeback): expose durability barriers and status"
```

---

### Task 9: Crash, corruption, and recovery integration gates

**Files:**
- Create: `zerofs/tests/writeback_recovery.rs`
- Create: `zerofs/tests/writeback_faults.rs`
- Modify: `zerofs/src/writeback/journal.rs`
- Modify: `zerofs/src/writeback/scheduler.rs`
- Modify: `zerofs/src/failpoints.rs`
- Modify: `zerofs/Cargo.toml`

**Interfaces:**
- Consumes: public test harness constructors, failpoints at temp-write/fsync/rename/dir-fsync/redb-commit/remote-reply boundaries.
- Produces: executable proof of the recovery contract and mutation-prefix invariants.

- [ ] **Step 1: Add failing process-level recovery tests**

Spawn the real ZeroFS binary with a local object-store backend and a temporary writeback directory. Test SIGKILL after memory acknowledgement, after local flush, during remote publication, and after remote publication before reply bookkeeping. Reopen and compare against literal expected namespaces and checksums.

- [ ] **Step 2: Add failing corruption and capacity tests**

Mutate one committed blob byte, delete one blob, replace a journal directory with a symlink, set an identity mismatch, force ENOSPC before reserve, cancel blocked writers, and terminate with workers active. Each case must fail closed with the exact typed category and leave unrelated committed records untouched.

- [ ] **Step 3: Run integration tests and record RED**

Run:

```bash
cargo test --test writeback_recovery -- --nocapture
cargo test --test writeback_faults --features failpoints -- --nocapture
```

Expected: each newly introduced crash boundary first exposes missing recovery behavior before the corresponding minimal fix.

- [ ] **Step 4: Implement only fixes required by each failing boundary**

For every failure, preserve the TDD receipt separately: test name, expected RED, minimal production change, and GREEN command. Do not weaken corruption assertions or delete failed journals during recovery.

- [ ] **Step 5: Run the complete verification matrix**

Run:

```bash
cargo fmt --all -- --check
cargo clippy -p zerofs --all-targets -- -D warnings
cargo test --lib
cargo test --bin zerofs
cargo test --test writeback_recovery -- --nocapture
cargo test --test writeback_faults --features failpoints -- --nocapture
git diff --check
gitleaks detect --no-banner --redact --source .
```

- [ ] **Step 6: Commit the slice**

```bash
git add zerofs/Cargo.toml zerofs/src/failpoints.rs zerofs/src/writeback zerofs/tests
git commit -m "test(writeback): prove crash recovery and fail-closed behavior"
```

---

### Task 10: Review, Linux build, CT105 pilot, VM100 acceptance, and rollback proof

**Files:**
- Create: `docs/superpowers/receipts/2026-08-11-persistent-writeback-verification.md`
- Modify: deployment config only after source review and clean push.

**Interfaces:**
- Consumes: reviewed branch commits, gitleaks receipt, CT105 build container, isolated Storage Box prefix, Proxmox host FUSE mount, VM100 VirtioFS mount.
- Produces: signed-off source revision, exact Linux binary hash, benchmark/recovery receipts, and a proven rollback path.

- [ ] **Step 1: Review the complete branch before push**

Audit `feat/sftp-object-store...HEAD` for scope, secrets, generated files, unsafe path handling, acknowledgement lies, sequence gaps, cancellation leaks, duplicate-daemon safety, and shutdown ordering. Address every Critical or Important finding through a new RED/GREEN cycle. Confirm `git status --short` is empty before push.

- [ ] **Step 2: Push and build the exact reviewed commit in CT105**

Fast-forward `/root/ZeroFS`, verify the exact commit hash, build a versioned release binary under a temporary target directory, record SHA-256, install without overwriting the retained rollback binary, and run `cargo clean` after copying the artifact.

- [ ] **Step 3: Run an isolated local-backend crash rehearsal**

Before Storage Box contact, run memory/SSD acknowledgement, SIGKILL, local recovery, remote barrier, corruption rejection, capacity hysteresis, and clean rollback tests against a throwaway local prefix and reduced budgets.

- [ ] **Step 4: Run an isolated Storage Box pilot**

Use a new dedicated prefix. Confirm no more than eight SFTP sessions and seven write operations. Measure:

- 4 GiB incompressible foreground write in memory mode, target at least 500 MiB/s;
- a workload above 16 GB transitioning from RAM to local SSD admission;
- a reduced SSD budget crossing high-water and falling to remote drain without ENOSPC;
- a queued remote drain target of at least 60 MB/s over a sustained sample;
- local and remote barrier latency and exact network byte counts.

- [ ] **Step 5: Prove CT restart and remote-only recovery**

After client fsync/local barrier, restart the service and verify checksums through the recovered journal. Then run `flush --remote`, cleanly stop, move the empty journal aside, start a read-only remote-backed instance, and verify the same checksums without the journal.

- [ ] **Step 6: Test VM100 through the canonical mount**

Retain host FUSE plus VirtioFS unless a separately measured alternative wins. Run large-file foreground write, random read, npm install, npm cold read, and `rm -rf` in a fresh `.bench/writeback-<UTC>` subtree. Record guest, host, CT, local-disk, and SFTP rates separately. Remove only the exact benchmark subtree afterward.

- [ ] **Step 7: Enable or roll back based on the acceptance gates**

Enable the canonical prefix only if every source, crash, capacity, remote-only, and VM100 gate passes. Otherwise run a remote barrier, prove zero pending bytes/ops, stop cleanly, restore the exact prior binary/config, verify the remote sentinel, and retain the non-empty journal if any barrier failed.

- [ ] **Step 8: Commit the verification receipt**

```bash
git add docs/superpowers/receipts/2026-08-11-persistent-writeback-verification.md
git commit -m "docs(writeback): record Linux and live acceptance receipts"
```

---

## Plan Self-Review Receipt

- Spec coverage: configuration, three watermarks, RAM admission, SSD durability, read overlay, all object mutations, multipart, ordering fences, reconciliation, identity, recovery, shutdown, RPC/CLI, metrics, CT105 deployment, VM100 validation, and rollback each map to a task.
- Placeholder scan: no deferred implementation markers or unspecified error-handling steps remain.
- Type consistency: `WritebackSettings`, `JournalIdentity`, `MutationRecord`, `WritebackObjectStore`, and `WritebackLifecycle` are introduced once and consumed by later tasks under the same names.
- Safety boundary: no live deployment begins until the reviewed source is clean, pushed, rebuilt on Linux, and passes local crash rehearsals.
