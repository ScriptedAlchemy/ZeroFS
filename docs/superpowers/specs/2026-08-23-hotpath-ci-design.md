# Hotpath pull-request profiling design

## Purpose

Add an optional pull-request performance signal for ZeroFS using the existing
feature-gated Hotpath integration. The signal compares the pull request head
with its base using the same real, self-contained 9P workload. It is advisory:
it helps reviewers spot timing changes but is not a production benchmark or a
merge gate.

The integration must preserve these boundaries:

- ordinary builds and deployments remain free of Hotpath instrumentation;
- profiling does not contact production, remote SFTP, NFS, or private storage;
- untrusted pull-request code never receives write permissions or secrets;
- a privileged commenting workflow never executes pull-request code or
  artifact content;
- every comparison is tied to the exact current head and base commits; and
- missing, malformed, stale, or non-comparable evidence fails closed instead
  of producing a reassuring comment.

## Evidence scope

The workload starts an isolated ZeroFS server with a `memory:///` object store
and a unique Unix 9P socket, then repeatedly runs the shipping
`zerofs-client/examples/quickstart.rs` client. That path performs real create,
write, fsync, positioned read/write, directory listing, statfs, and cleanup
operations through the production 9P client and server.

The comparison is explicitly named `zerofs-9p-memory`. It is evidence about
function timing in this one 9P plus in-memory-backend workload. It is not
evidence about SFTP, NFS, SSD or remote durability, WAN throughput, production
latency, cancellation safety, or mounted-filesystem performance.

Hotpath 0.23.3's `profile-pr` utility compares function timing, allocation, and
thread sections, but not futures or I/O sections. The CI workload therefore
records `functions-timing` only and requires the stable
`zerofs.extent.read` label. Richer futures, SFTP I/O, and thread evidence
remains available through the separately maintained VM100 profile workflow.

## Architecture

### Unprivileged profile workflow

A `pull_request` workflow receives only `contents: read`. It checks out and
profiles the exact pull-request head and base commits in separate directories
within one job. Keeping both measurements on one runner reduces environmental
variance and avoids two independent release-build queues.

For each revision, a repository-owned Python orchestrator:

1. builds the ZeroFS release binary with `hotpath-profile` and builds the
   release quickstart example;
2. creates a unique temporary cache, config, socket, log, and report directory;
3. starts ZeroFS with telemetry disabled and these exact Hotpath controls:
   `HOTPATH_OUTPUT_FORMAT=json`, `HOTPATH_REPORT=functions-timing`,
   `HOTPATH_CPU_BASELINE_OFF=true`,
   `HOTPATH_FUNCTIONS_TIME_SAMPLING_RATE=1`, and
   `HOTPATH_METRICS_SERVER_OFF=true`;
4. waits with a bounded deadline for the Unix socket;
5. executes a fixed number of complete quickstart iterations;
6. terminates ZeroFS normally and waits with a bounded deadline so Hotpath can
   flush its JSON report;
7. validates process results, workload output, report schema, exact required
   labels and numeric invariants, and absence of owned workload paths; and
8. removes its entire unique temporary directory on success or failure.

The orchestrator emits a small receipt containing the benchmark identifier,
revision, iteration count, elapsed wall time, report SHA-256, required label,
and cleanup result. It never includes raw paths, credentials, environment
values, or application data.

The workflow validates a metadata manifest binding the artifact to repository,
workflow run, pull request, head SHA, base SHA, benchmark identifier, report
hashes, and schema version. It uploads only the two reports, receipts, and
manifest with one-day retention and `if-no-files-found: error`.

### Privileged comment workflow

A separate `workflow_run` workflow runs only after the named profile workflow
finishes successfully. It receives `actions: read` and `pull-requests: write`;
it does not receive `contents: write`, does not checkout any repository, and
does not restore pull-request caches.

The comment job:

1. downloads the artifact into a new directory under `RUNNER_TEMP`;
2. treats every artifact byte as attacker-controlled data;
3. rejects symlinks, unexpected files, path traversal, oversized files,
   duplicate names, malformed JSON, unknown schema versions, invalid hashes,
   and invalid identifiers;
4. queries GitHub for the pull request and requires it to remain open in this
   repository with current head and base SHAs exactly matching the manifest;
5. installs the schema-matched `hotpath-utils =0.23.3` from crates.io using
   `--locked` and an explicitly pinned Rust toolchain;
6. runs `profile-pr --dry-run` only, without a GitHub token;
7. validates the generated Markdown contains the expected comparison heading,
   benchmark identifier, and `zerofs.extent.read` row, and rejects the
   utility's successful-but-empty `No comparable sections found` output; and
8. uses a SHA-pinned `actions/github-script` to create or update exactly one
   comment identified by `<!-- zerofs-hotpath-ci:9p-memory -->`.

The privileged job parses reports as data and never imports, evaluates,
sources, shells, or executes artifact content. A stale run exits successfully
with an explicit skip notice in the job summary and does not alter comments.
Every other evidence failure fails the job without posting.

## Workflow security

All third-party actions are pinned to full commit SHAs. Checkout disables
credential persistence. No workflow uses `pull_request_target`. Shell inputs
come from validated files or trusted GitHub event fields, not directly from
pull-request titles, branch names, labels, or artifact strings. Workflows set
explicit job timeouts and concurrency groups that cancel superseded runs.

The pull-request workflow may execute untrusted code because it has no secrets
or write token. The commenting workflow has write authority but cannot execute
that code. This split follows GitHub's untrusted-build/privileged-consumer
model while adding stricter report and revision validation than the upstream
Hotpath example.

## Failure and cancellation behavior

The orchestrator owns the entire server process group. On failure, timeout, or
interrupt it sends a bounded termination sequence, waits for descendants,
preserves diagnostic logs only in the unprivileged job, and verifies its Unix
socket, cache, and workload directory are gone. It never reports success when
the server cannot be reaped or cleanup cannot be proved.

Head and base must both succeed. There is no empty-report fallback, synthetic
baseline, prior-run substitution, or `continue-on-error` path. The comparison
is advisory only after both real runs pass.

## Repository changes

The Hotpath CI pull request is expected to contain:

- `.github/workflows/hotpath-profile.yml`;
- `.github/workflows/hotpath-comment.yml`;
- `scripts/hotpath_ci_profile.py`;
- focused tests under `scripts/tests/` for orchestration, report validation,
  manifest validation, cancellation, and cleanup;
- static workflow contract tests for permissions, event matrices, action pins,
  stale-SHA checks, and the no-checkout privileged job; and
- concise additions to `README.md` and `AGENTS.md` documenting how to run and
  interpret the workload.

The NFS kernel-build timeout correction is intentionally excluded and will be
opened as a separate CI-debt pull request.

## Test strategy

Implementation begins with RED tests for:

- report absence, malformed schema, missing label, invalid numeric fields, and
  hash mismatch;
- server startup failure, quickstart failure, graceful-shutdown timeout,
  forced-reap failure, and cleanup failure;
- exact head/base and benchmark manifest binding;
- stale or closed pull requests;
- artifact symlinks, traversal, extra files, duplicates, and size limits;
- empty `profile-pr` output and missing Markdown evidence;
- unprivileged and privileged workflow permission/event matrices; and
- absence of Hotpath behavior from ordinary builds and deployments.

Ubuntu acceptance then runs the full focused Python suite, Ruff, py_compile,
actionlint, workflow contract tests, release builds, a real head/base 9P-memory
profile, exact Hotpath 0.23.3 dry-run comparison, diff checks, and an
independent security/correctness review. The pull request remains separate from
production deployment and does not modify CT198.

## Rollout

The first pull request opens ready for review after local Ubuntu evidence is
green. The workflow comment is informational and must describe its limited
scope. It is not added to required branch protection until repeated runs show
stable execution and useful signal. Any later threshold or merge-gating policy
requires a separate decision backed by observed variance.
