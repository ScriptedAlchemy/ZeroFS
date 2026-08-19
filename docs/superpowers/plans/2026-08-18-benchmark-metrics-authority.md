# Benchmark Metrics Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production, opt-in HTTPS benchmark metrics authority whose one immutable response tuple identifies the exact ZeroFS process, filesystem namespace, and isolated export.

**Architecture:** Extend `PrometheusConfig` with a strictly validated one-export authority block. Compose its durable bucket identity and process identity in the real server startup path, render exactly one authority info sample beside the existing metrics snapshot, and serve canonical `/metrics` over rustls-backed HTTPS while leaving disabled mode plaintext-compatible.

**Tech Stack:** Rust 2024, Serde/TOML, Tokio, Hyper HTTP/1, rustls 0.23, tokio-rustls 0.26, metrics-exporter-prometheus, ZeroFS object store, Python unittest cross-contract lane.

**Spec:** `docs/superpowers/specs/2026-08-18-benchmark-metrics-authority-design.md`

## Global Constraints

- Emit exactly one `zerofs_benchmark_authority_info{server_instance_id="...",filesystem_id="...",export_id="..."} 1` sample in every successful authority response.
- Do not add authority labels to existing series.
- Accept authority label values only when non-empty and every byte matches `[A-Za-z0-9._:/-]+`.
- Use validated systemd `INVOCATION_ID` when present and one random process UUID otherwise.
- Load `filesystem_id` from the existing durable `<database-prefix>/.zerofs_bucket_id`; do not synthesize a second filesystem identity.
- Require one selected NFS or 9P export and fail closed for unsupported multi-export authority mode.
- Enabled mode exposes only TLS `GET /metrics` with no query; disabled mode preserves plaintext behavior.
- TLS uses normal certificates, rejects missing/unreadable/malformed/mismatched material, and rejects group/world-readable private keys on Unix.
- No redirects, downgrade, URL credentials, query, or fragment.
- No live deployment.

---

### Task 1: Authority configuration and isolated export validation

**Files:**
- Modify: `zerofs/src/config.rs`
- Test: `zerofs/src/config.rs` test module

**Interfaces:**
- Produces: `BenchmarkAdapter::{Nfs,Ninep}`.
- Produces: `BenchmarkAuthorityConfig { adapter, export_id, tls_certificate, tls_private_key }`.
- Produces: `PrometheusConfig::validate(&ServerConfig) -> anyhow::Result<()>` and `BenchmarkAuthorityConfig::validate_label(&str, &str) -> anyhow::Result<()>`.

- [ ] **Step 1: Write failing config tests**

Add table-driven tests proving a valid single NFS source loads, invalid label characters fail, a selected adapter must exist, NFS and 9P cannot coexist, multiple selected endpoints fail, and NFS `export_id` must equal the canonical source derived from its sole non-wildcard listener.

```rust
assert_eq!(authority.export_id, "10.10.10.30:/");
for invalid in ["", "host:/?x=1", "user@host:/", "host:/\nother"] {
    assert!(BenchmarkAuthorityConfig::validate_label(invalid, "export_id").is_err());
}
```

- [ ] **Step 2: Run the focused config tests and verify RED**

Run: `cargo test --manifest-path zerofs/Cargo.toml config::tests::benchmark_authority -- --nocapture`

Expected: compilation failure because the authority configuration types and field do not exist.

- [ ] **Step 3: Implement the minimal Serde types and validation**

Add the nested optional `benchmark_authority` field to `PrometheusConfig`, validate it from `Settings::validate`, and keep the field absent by default so existing configurations serialize and behave unchanged.

- [ ] **Step 4: Run focused config tests and verify GREEN**

Run: `cargo test --manifest-path zerofs/Cargo.toml config::tests::benchmark_authority -- --nocapture`

Expected: all authority configuration tests pass.

- [ ] **Step 5: Commit the green configuration slice**

```bash
git add zerofs/src/config.rs
git commit -m "feat(metrics): validate benchmark authority config"
```

### Task 2: Durable filesystem and immutable process identity

**Files:**
- Modify: `zerofs/src/bucket_identity.rs`
- Modify: `zerofs/src/prometheus.rs`
- Test: the test modules in both files

**Interfaces:**
- Produces: `BucketIdentity::load(object_store, db_path) -> anyhow::Result<BucketIdentity>`.
- Produces: `BenchmarkAuthority { server_instance_id, filesystem_id, export_id }`.
- Produces: `BenchmarkAuthority::compose(config, bucket_identity, invocation_id) -> anyhow::Result<Self>` where the invocation argument is injectable for deterministic tests and production passes `std::env::var("INVOCATION_ID")` state.

- [ ] **Step 1: Write failing bucket marker tests**

Add real in-memory object-store tests proving `load` returns the existing marker unchanged across calls and rejects missing or malformed markers without creating them.

```rust
let first = BucketIdentity::load(&store, "data").await.unwrap();
let second = BucketIdentity::load(&store, "data").await.unwrap();
assert_eq!(first.id(), second.id());
```

- [ ] **Step 2: Run bucket tests and verify RED**

Run: `cargo test --manifest-path zerofs/Cargo.toml bucket_identity::tests::load_ -- --nocapture`

Expected: compilation failure because `BucketIdentity::load` does not exist.

- [ ] **Step 3: Implement marker-only load and verify GREEN**

Reuse the existing marker parse logic, but return an error on `NotFound`; do not call `get_or_create`.

Run: `cargo test --manifest-path zerofs/Cargo.toml bucket_identity::tests::load_ -- --nocapture`

- [ ] **Step 4: Write failing process identity tests**

Test that a valid supplied invocation ID wins exactly, an invalid present value errors, one composed authority remains immutable, and two absent-invocation compositions receive different UUID values.

- [ ] **Step 5: Run process identity tests and verify RED**

Run: `cargo test --manifest-path zerofs/Cargo.toml prometheus::tests::benchmark_authority_identity -- --nocapture`

Expected: compilation failure because `BenchmarkAuthority` is absent.

- [ ] **Step 6: Implement identity composition and verify GREEN**

Create the UUID once per composition and validate every final label through the shared validator.

Run both focused identity test commands and expect all tests to pass.

- [ ] **Step 7: Commit the green identity slice**

```bash
git add zerofs/src/bucket_identity.rs zerofs/src/prometheus.rs
git commit -m "feat(metrics): compose durable benchmark identity"
```

### Task 3: Exact production text and canonical request routing

**Files:**
- Modify: `zerofs/src/prometheus.rs`
- Test: `zerofs/src/prometheus.rs` test module

**Interfaces:**
- Produces: `render_metrics(&PrometheusHandle, Option<&BenchmarkAuthority>) -> String`.
- Produces: `handle_request(request, handle, authority) -> HttpResponse`.

- [ ] **Step 1: Write failing exact render tests**

Register an ordinary counter, render twice, and assert each body has exactly one literal authority sample, identical tuples, and no authority label on the ordinary counter. Include a mutation guard that counts non-comment sample names rather than source text.

```rust
assert_eq!(sample_count(&body, "zerofs_benchmark_authority_info"), 1);
assert!(body.contains("zerofs_bytes_read_total 7"));
assert!(!body.contains("zerofs_bytes_read_total{"));
```

- [ ] **Step 2: Run render tests and verify RED**

Run: `cargo test --manifest-path zerofs/Cargo.toml prometheus::tests::benchmark_authority_response -- --nocapture`

Expected: missing renderer/authority behavior.

- [ ] **Step 3: Implement minimal response rendering**

Append one literal info sample after the recorder output, using only prevalidated labels; do not register the info metric in `metrics`.

- [ ] **Step 4: Write and run failing routing tests**

Test success only for `GET /metrics`; `POST /metrics`, `/metrics?x=1`, `/metrics/`, `/`, and an `Authorization` header must not return 200 and must not expose metrics.

- [ ] **Step 5: Implement canonical routing and verify GREEN**

Keep legacy disabled-mode routing behavior except for the existing path rule; apply strict method/query/authorization checks when authority is present.

Run all `prometheus::tests::benchmark_authority_` tests and expect pass.

- [ ] **Step 6: Commit the green response contract**

```bash
git add zerofs/src/prometheus.rs
git commit -m "feat(metrics): emit canonical authority response"
```

### Task 4: TLS-only authority listener in real startup

**Files:**
- Modify: `zerofs/Cargo.toml`
- Modify: `zerofs/Cargo.lock`
- Modify: `zerofs/src/prometheus.rs`
- Modify: `zerofs/src/cli/server.rs`
- Test: `zerofs/src/prometheus.rs` test module
- Test: `zerofs/src/cli/server.rs` test module if startup seam coverage is required

**Interfaces:**
- Consumes: validated `BenchmarkAuthorityConfig` and composed `BenchmarkAuthority`.
- Produces: async `prometheus::start(...) -> anyhow::Result<Vec<JoinHandle<()>>>`.
- Produces: rustls-backed listener tasks bound before `start` returns.

- [ ] **Step 1: Write failing TLS material tests**

Use generated test certificates or checked-in test-only PEM fixtures to prove valid material loads, mismatched/empty material fails, and Unix private-key mode `0644` fails while `0600` succeeds.

- [ ] **Step 2: Run TLS material tests and verify RED**

Run: `cargo test --manifest-path zerofs/Cargo.toml prometheus::tests::benchmark_authority_tls -- --nocapture`

Expected: loader is missing.

- [ ] **Step 3: Add direct tokio-rustls dependency and implement TLS loading**

Use rustls 0.23 PEM types, `ServerConfig::builder().with_no_client_auth().with_single_cert(...)`, and Unix metadata permission checks. Do not weaken protocol or certificate validation.

- [ ] **Step 4: Write a failing real-listener integration test**

Bind `127.0.0.1:0`, trust the generated test CA in a rustls client, request `GET /metrics`, and assert the exact authority line. Attempt plaintext HTTP against the same port and assert it cannot receive a metrics response.

- [ ] **Step 5: Implement the TLS accept loop and verify GREEN**

Wrap accepted Tokio TCP streams with `TlsAcceptor` before passing them to Hyper. Log and close failed handshakes; never hand the raw stream to HTTP in authority mode.

- [ ] **Step 6: Write a failing server composition test**

Exercise the production composition seam with an in-memory durable bucket marker and assert missing marker/TLS prevents `start` from returning handles.

- [ ] **Step 7: Compose identity before moving `InitResult` fields**

In `run_server`, load the marker from `init_result.object_store` and `init_result.db_path`, construct one authority, then `await` exporter startup. Propagate all errors with context before other data-plane listeners enter the serving select loop.

- [ ] **Step 8: Run focused server/exporter tests and verify GREEN**

Run the authority config, bucket identity, exporter, and server listener tests.

- [ ] **Step 9: Commit the authority TLS startup slice**

```bash
git add zerofs/Cargo.toml zerofs/Cargo.lock zerofs/src/prometheus.rs zerofs/src/cli/server.rs
git commit -m "feat(metrics): serve benchmark authority over TLS"
```

### Task 5: Python MetricsClient cross-contract reconciliation

**Files:**
- Read/verify only in this branch: `scripts/vm100_pilot/metrics.py`
- Read/verify only in this branch: `scripts/tests/test_vm100_pilot.py`
- Coordinate with: benchmark harness modernization lane

**Interfaces:**
- Rust produces the exact one-line authority schema.
- Python provides `MetricsClient.identity()` and a validated `snapshot()` bound to the same response identity.

- [ ] **Step 1: Send the exact Rust rendered fixture and SHA to the harness owner**

The fixture must include ordinary labeled/unlabeled Prometheus lines plus exactly one authority sample so the parser cannot assume all sample names lack labels.

- [ ] **Step 2: Verify the Python owner’s RED/GREEN evidence**

Require tests for HTTP/userinfo/query/fragment rejection, redirect refusal, default CA/hostname verification, missing/duplicate/malformed authority rejection, and the exact Rust fixture.

- [ ] **Step 3: Run the integrated Python tests after merge-order reconciliation**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot`

Expected: all tests pass with production `MetricsClient`; do not copy or fork the client implementation in this branch.

### Task 6: Separate legacy bind-failure hardening

**Files:**
- Modify: `zerofs/src/prometheus.rs`
- Modify: `zerofs/src/cli/server.rs`
- Test: `zerofs/src/prometheus.rs` test module

**Interfaces:**
- Changes disabled plaintext mode so initial bind failures propagate from `prometheus::start` rather than being logged only in a background task.

- [ ] **Step 1: Write a failing occupied-port test**

Bind a test `TcpListener`, configure legacy plaintext Prometheus on its address, and assert `start` returns an address-in-use error without task handles.

- [ ] **Step 2: Verify RED, implement prebinding for disabled mode, verify GREEN**

Keep request and transport behavior unchanged; only move bind before task spawn and return the error.

- [ ] **Step 3: Commit this widened semantic separately**

```bash
git add zerofs/src/prometheus.rs zerofs/src/cli/server.rs
git commit -m "fix(metrics): fail startup on exporter bind errors"
```

### Task 7: Linux-quality verification and review

**Files:**
- Modify only if verification reveals an authority-slice defect.

**Interfaces:**
- Produces: final green command receipts, overlap report, merge order, and separate GPT-5.6-sol xhigh review findings.

- [ ] **Step 1: Run formatting**

Run: `cargo fmt --manifest-path zerofs/Cargo.toml -- --check`

- [ ] **Step 2: Run focused Rust tests**

Run all benchmark authority, bucket identity, config, and server listener test filters with `--nocapture`.

- [ ] **Step 3: Run strict Clippy**

Run: `cargo clippy --manifest-path zerofs/Cargo.toml --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Run the relevant full Rust suite**

Run: `cargo test --manifest-path zerofs/Cargo.toml --all-features`

- [ ] **Step 5: Run integrated Python tests on the reconciled merge descendant**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot`

- [ ] **Step 6: Dispatch a separate GPT-5.6-sol xhigh final review**

Ask for security/correctness findings only, with special attention to TLS downgrade, identity mutability, duplicate info samples, startup error propagation, one-export validation, and test vacuity. Fix every confirmed authority-slice defect with a failing regression test first.

- [ ] **Step 7: Report exact integration receipts**

Report base SHA, every incremental commit SHA, dirty status, overlapping files (`metrics.py`/Python tests owned by the harness lane), and merge order: Rust authority commits first, harness MetricsClient commit second, then run the combined gates on the descendant.
