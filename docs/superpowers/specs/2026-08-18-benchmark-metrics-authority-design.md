# Benchmark Metrics Authority Design

## Purpose

ZeroFS benchmark results must be bound to the exact running server process,
filesystem namespace, and mounted export that produced them. The current
Prometheus endpoint is unauthenticated plaintext and contains no stable
identity, so a harness can accidentally sample another process, namespace, or
export and still produce plausible results.

This change adds an opt-in benchmark-authority mode to the production
Prometheus exporter. Disabled mode preserves the existing plaintext endpoint.
Enabled mode starts only after TLS and identity are valid and exposes only the
canonical HTTPS `/metrics` resource.

## Configuration

Authority mode is configured below `[prometheus]`:

```toml
[prometheus]
addresses = ["10.10.10.30:9567"]

[prometheus.benchmark_authority]
adapter = "nfs"
export_id = "10.10.10.30:/"
tls_certificate = "/etc/zerofs/metrics.crt"
tls_private_key = "/etc/zerofs/metrics.key"
```

`adapter` is `nfs` or `ninep`. Authority mode supports one configured export
adapter and one Prometheus listen address. It rejects configurations with both
NFS and 9P listeners, with a missing selected adapter, or with multiple selected
adapter endpoints. This restriction is intentional: the single `export_id`
cannot honestly identify a multi-export response.

`export_id` is the exact source the benchmark host observes from
`findmnt -nro SOURCE -M <mountpoint>`. NFS validation requires the configured
single non-wildcard listener address and the canonical `<host>:/` source. 9P
validation requires the exact client-visible source because the 9P mount source
is chosen by the client and cannot be reconstructed by the server; the server
still verifies that exactly one 9P endpoint is configured and that the value is
a valid authority label.

Authority labels accept only non-empty ASCII values matching
`[A-Za-z0-9._:/-]+`. This makes Prometheus rendering literal and rejects label
escaping, newlines, credentials, query strings, fragments, and ambiguous
values.

`tls_certificate` and `tls_private_key` are required absolute paths. Startup
reads and parses the entire certificate chain and one supported PEM private key,
verifies that the key matches the certificate through rustls configuration,
and rejects a group- or world-accessible private key on Unix. A missing,
unreadable, empty, malformed, mismatched, or insecure key/certificate prevents
the ZeroFS server from entering its serving runtime.

## Identity Composition

The exporter constructs one immutable `BenchmarkAuthority` before binding its
listener:

- `server_instance_id` is the systemd `INVOCATION_ID` when that environment
  variable is present and passes the label validator. Outside systemd it is a
  random UUID generated once for the process. An invalid present
  `INVOCATION_ID` fails startup instead of silently substituting another
  identity.
- `filesystem_id` is loaded from the existing durable
  `<database-prefix>/.zerofs_bucket_id` object. Authority mode does not invent a
  second identifier and does not create a missing marker during server startup.
  A missing or malformed marker fails startup.
- `export_id` is the validated configured scenario source described above.

The process value is created once and shared by every authority listener and
response. It is stable throughout one process and changes on a non-systemd
restart. The filesystem value survives process restarts because it is stored in
the namespace. The export value changes only when the configured source
changes.

## Metrics Response Contract

Every successful authority response contains exactly one sample:

```text
zerofs_benchmark_authority_info{server_instance_id="...",filesystem_id="...",export_id="..."} 1
```

The line is rendered by the production exporter after the ordinary metrics
snapshot. Existing metrics are not given these labels, avoiding a cardinality
and dashboard compatibility change. The unique info sample binds the complete
response body to the tuple; duplicate info samples are impossible because the
authority metric is not registered in the global recorder.

Only `GET /metrics` with no query is successful. Other paths and methods,
including `/metrics?x=y`, receive an error response. The authority listener
performs TLS before HTTP, never starts a plaintext listener, never redirects,
and does not accept or emit credentials. A plaintext client therefore cannot
downgrade the connection.

## Startup and Supervision

Prometheus startup becomes fallible. In authority mode it validates adapter
scope, loads the durable filesystem identity, loads TLS, and binds every
configured address before returning task handles. Any failure propagates out of
the real `zerofs server` startup path. Existing non-authority Prometheus mode
continues to bind and serve plaintext as before, but bind failures also become
startup errors rather than background log-only failures.

Accepted TLS connections are served by the existing Hyper HTTP/1 service over
`tokio-rustls`. A failed individual TLS handshake is logged without terminating
the listener. Cancellation stops listener loops through the existing server
shutdown token.

## Verification

Rust tests drive the contract test-first and cover:

- label acceptance and rejection;
- systemd invocation selection, invalid-present failure, process immutability,
  and UUID fallback restart change;
- durable bucket marker stability and missing/malformed failure;
- one-export adapter validation and exact NFS source matching;
- TLS certificate/key loading, private-key permissions, and mismatched keys;
- response rendering with exactly one info sample and no tuple labels added to
  ordinary metrics;
- canonical request routing and a real TLS listener handshake where plaintext
  access fails.

The Python harness lane consumes the exact rendered Rust fixture and verifies
that its production `MetricsClient` requires canonical credential-free HTTPS,
uses the default CA and hostname checks, refuses redirects, requires exactly one
authority sample, and binds snapshots to the same response identity. Linux
verification runs focused tests, the Python harness suite, `cargo fmt --check`,
strict Clippy with warnings denied, and the relevant server tests.

No live deployment or service mutation is part of this slice.
