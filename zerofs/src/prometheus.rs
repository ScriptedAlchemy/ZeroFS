use crate::cache_metrics::{CacheMetrics, CacheMetricsSnapshot, CacheTierSnapshot};
use crate::config::PrometheusConfig;
use crate::dedup::DedupCache;
use crate::fs::metrics::{FileSystemStats, SegmentGcStats};
use crate::fs::stats::FileSystemGlobalStats;
use crate::task::spawn_named;
use crate::writeback::model::WritebackStatus;
use crate::writeback::store::WritebackObjectStore;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const GENERAL_COLLECT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const WRITEBACK_COLLECT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkAuthority {
    pub server_instance_id: String,
    pub filesystem_id: String,
    pub export_id: String,
}

impl BenchmarkAuthority {
    pub fn compose(
        export_id: &str,
        filesystem_id: uuid::Uuid,
        invocation_id: Option<&str>,
    ) -> anyhow::Result<Self> {
        let server_instance_id = invocation_id.map_or_else(
            || uuid::Uuid::new_v4().to_string(),
            std::borrow::ToOwned::to_owned,
        );
        let filesystem_id = filesystem_id.to_string();
        let export_id = export_id.to_owned();
        for (role, value) in [
            ("server_instance_id", server_instance_id.as_str()),
            ("filesystem_id", filesystem_id.as_str()),
            ("export_id", export_id.as_str()),
        ] {
            crate::config::BenchmarkAuthorityConfig::validate_label(value, role)?;
        }
        Ok(Self {
            server_instance_id,
            filesystem_id,
            export_id,
        })
    }

    pub async fn load(
        export_id: &str,
        object_store: &Arc<dyn object_store::ObjectStore>,
        db_path: &str,
        invocation_id: Option<&str>,
    ) -> anyhow::Result<Self> {
        let filesystem_id = crate::bucket_identity::BucketIdentity::load(object_store, db_path)
            .await?
            .id();
        Self::compose(export_id, filesystem_id, invocation_id)
    }
}

pub fn systemd_invocation_id() -> anyhow::Result<Option<String>> {
    let Some(value) = std::env::var_os("INVOCATION_ID") else {
        return Ok(None);
    };
    value.into_string().map(Some).map_err(|_| {
        anyhow::anyhow!(
            "systemd INVOCATION_ID is not valid Unicode and cannot identify benchmark metrics"
        )
    })
}

/// Start the Prometheus metrics exporter.
///
/// Installs the global metrics recorder, spawns an HTTP server per configured address
/// serving `/metrics`, and starts a background collector task that bridges existing
/// ZeroFS and SlateDB stats into the metrics crate.
pub struct CollectorSources {
    pub fs_stats: Arc<FileSystemStats>,
    pub global_stats: Arc<FileSystemGlobalStats>,
    pub segment_gc_stats: Arc<SegmentGcStats>,
    pub dedup: Arc<DedupCache>,
    pub cache_metrics: Arc<crate::cache_metrics::CacheMetrics>,
    pub slatedb_registry: Option<Arc<DefaultMetricsRecorder>>,
    pub writeback: Option<WritebackObjectStore>,
}

pub async fn start(
    config: &PrometheusConfig,
    sources: CollectorSources,
    authority: Option<BenchmarkAuthority>,
    shutdown: CancellationToken,
) -> anyhow::Result<Vec<JoinHandle<()>>> {
    let prepared_authority = prepare_authority_server(config, authority).await?;
    let plaintext_listeners = if prepared_authority.is_none() {
        bind_plaintext_listeners(config).await?
    } else {
        Vec::new()
    };
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    metrics::set_global_recorder(recorder).expect("failed to install Prometheus recorder");

    let mut handles = Vec::new();

    if let Some(prepared) = prepared_authority {
        let PreparedAuthorityServer {
            listeners,
            tls,
            authority,
        } = prepared;
        for listener in listeners {
            let address = listener
                .local_addr()
                .expect("bound benchmark authority listener has a local address");
            tracing::info!(
                "Benchmark authority metrics server listening on https://{}/metrics",
                address
            );
            let server_handle = handle.clone();
            let server_shutdown = shutdown.clone();
            let server_authority = authority.clone();
            let tls = Arc::clone(&tls);
            handles.push(spawn_named("prometheus-https-authority", async move {
                serve_tls_metrics(
                    listener,
                    server_handle,
                    server_authority,
                    tls,
                    server_shutdown,
                )
                .await;
            }));
        }
    } else {
        for listener in plaintext_listeners {
            let address = listener
                .local_addr()
                .expect("bound Prometheus listener has a local address");
            tracing::info!(
                "Prometheus metrics server listening on http://{}/metrics",
                address
            );
            let server_handle = handle.clone();
            let server_shutdown = shutdown.clone();
            handles.push(spawn_named("prometheus-http", async move {
                serve_metrics(listener, server_handle, None, server_shutdown).await;
            }));
        }
    }

    let CollectorSources {
        fs_stats,
        global_stats,
        segment_gc_stats,
        dedup,
        cache_metrics,
        slatedb_registry,
        writeback,
    } = sources;
    let writeback_source = writeback.clone();
    let collector_shutdown = shutdown.clone();
    let upkeep_handle = handle.clone();
    handles.push(spawn_named("prometheus-collector", async move {
        let mut interval = tokio::time::interval(GENERAL_COLLECT_INTERVAL);
        loop {
            tokio::select! {
                _ = collector_shutdown.cancelled() => {
                    tracing::info!("Prometheus collector shutting down");
                    break;
                }
                _ = interval.tick() => {
                    collect_fs_stats(&fs_stats);
                    collect_global_stats(&global_stats);
                    collect_segment_gc_stats(&segment_gc_stats);
                    collect_dedup_stats(&dedup);
                    collect_cache_metrics(&cache_metrics);
                    if let Some(ref registry) = slatedb_registry {
                        collect_lsm_stats(registry);
                    }
                    collect_jemalloc_stats();
                    upkeep_handle.run_upkeep();
                }
            }
        }
    }));

    handles.push(spawn_named("prometheus-writeback", async move {
        let mut interval = tokio::time::interval(WRITEBACK_COLLECT_INTERVAL);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Prometheus writeback collector shutting down");
                    break;
                }
                _ = interval.tick() => {
                    collect_writeback_stats(writeback_source.as_ref());
                }
            }
        }
    }));

    Ok(handles)
}

async fn bind_plaintext_listeners(
    config: &PrometheusConfig,
) -> anyhow::Result<Vec<tokio::net::TcpListener>> {
    use anyhow::Context;

    if config.benchmark_authority.is_some() {
        anyhow::bail!("plaintext metrics listeners cannot be prepared in authority mode");
    }
    let mut listeners = Vec::with_capacity(config.addresses.len());
    for address in &config.addresses {
        listeners.push(
            tokio::net::TcpListener::bind(address)
                .await
                .with_context(|| format!("failed to bind Prometheus metrics at {address}"))?,
        );
    }
    Ok(listeners)
}

struct PreparedAuthorityServer {
    listeners: Vec<tokio::net::TcpListener>,
    tls: Arc<rustls::ServerConfig>,
    authority: BenchmarkAuthority,
}

async fn prepare_authority_server(
    config: &PrometheusConfig,
    authority: Option<BenchmarkAuthority>,
) -> anyhow::Result<Option<PreparedAuthorityServer>> {
    use anyhow::Context;

    let authority_config = match (&config.benchmark_authority, authority) {
        (None, None) => return Ok(None),
        (None, Some(_)) => {
            anyhow::bail!(
                "benchmark authority identity was supplied while authority mode is disabled"
            )
        }
        (Some(_), None) => {
            anyhow::bail!("benchmark authority is enabled but its identity is missing")
        }
        (Some(authority_config), Some(authority)) => (authority_config, authority),
    };
    let tls = load_tls_config(authority_config.0)?;
    let mut listeners = Vec::with_capacity(config.addresses.len());
    for address in &config.addresses {
        listeners.push(
            tokio::net::TcpListener::bind(address)
                .await
                .with_context(|| {
                    format!("failed to bind benchmark authority metrics at {address}")
                })?,
        );
    }
    Ok(Some(PreparedAuthorityServer {
        listeners,
        tls,
        authority: authority_config.1,
    }))
}

type HttpResponse = hyper::Response<http_body_util::Full<bytes::Bytes>>;

fn render_metrics(
    metrics_handle: &PrometheusHandle,
    authority: Option<&BenchmarkAuthority>,
) -> String {
    let mut rendered = metrics_handle.render();
    if let Some(authority) = authority {
        if !rendered.is_empty() && !rendered.ends_with('\n') {
            rendered.push('\n');
        }
        use std::fmt::Write;
        writeln!(
            rendered,
            "zerofs_benchmark_authority_info{{server_instance_id=\"{}\",filesystem_id=\"{}\",export_id=\"{}\"}} 1",
            authority.server_instance_id, authority.filesystem_id, authority.export_id
        )
        .expect("writing metrics to a String cannot fail");
    }
    rendered
}

fn handle_request(
    req: hyper::Request<impl hyper::body::Body>,
    metrics_handle: &PrometheusHandle,
    authority: Option<&BenchmarkAuthority>,
) -> HttpResponse {
    let canonical_authority_request = authority.is_none()
        || (req.method() == hyper::Method::GET
            && req.uri().query().is_none()
            && !req.headers().contains_key(hyper::header::AUTHORIZATION));
    if req.uri().path() != "/metrics" || !canonical_authority_request {
        return hyper::Response::builder()
            .status(404)
            .body(http_body_util::Full::new(bytes::Bytes::from("Not Found")))
            .unwrap();
    }

    hyper::Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(http_body_util::Full::new(bytes::Bytes::from(
            render_metrics(metrics_handle, authority),
        )))
        .unwrap()
}

fn load_tls_config(
    config: &crate::config::BenchmarkAuthorityConfig,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    use anyhow::Context;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    fn require_regular_file(path: &Path, role: &str) -> anyhow::Result<std::fs::Metadata> {
        let metadata = std::fs::metadata(path).with_context(|| {
            format!(
                "failed to read benchmark authority {role} {}",
                path.display()
            )
        })?;
        if !metadata.is_file() {
            anyhow::bail!(
                "benchmark authority {role} must be a regular file: {}",
                path.display()
            );
        }
        Ok(metadata)
    }

    require_regular_file(&config.tls_certificate, "TLS certificate")?;
    let private_key_metadata = require_regular_file(&config.tls_private_key, "TLS private key")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = private_key_metadata.permissions().mode();
        if mode & 0o077 != 0 {
            anyhow::bail!(
                "benchmark authority TLS private key permissions must deny group/world access: {} has mode {:04o}",
                config.tls_private_key.display(),
                mode & 0o7777
            );
        }
    }

    let certificates = CertificateDer::pem_file_iter(&config.tls_certificate)
        .with_context(|| {
            format!(
                "failed to open benchmark authority TLS certificate {}",
                config.tls_certificate.display()
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| {
            format!(
                "failed to parse benchmark authority TLS certificate chain {}",
                config.tls_certificate.display()
            )
        })?;
    if certificates.is_empty() {
        anyhow::bail!(
            "benchmark authority TLS certificate chain is empty: {}",
            config.tls_certificate.display()
        );
    }
    let private_key = PrivateKeyDer::from_pem_file(&config.tls_private_key).with_context(|| {
        format!(
            "failed to parse benchmark authority TLS private key {}",
            config.tls_private_key.display()
        )
    })?;

    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .context("benchmark authority TLS private key does not match certificate chain")?;
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(tls))
}

async fn serve_metrics(
    listener: tokio::net::TcpListener,
    handle: PrometheusHandle,
    authority: Option<BenchmarkAuthority>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            result = listener.accept() => {
                let (stream, _) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::debug!("Prometheus accept error: {}", e);
                        continue;
                    }
                };
                let handle = handle.clone();
                let authority = authority.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        std::future::ready(Ok::<_, std::convert::Infallible>(
                            handle_request(req, &handle, authority.as_ref()),
                        ))
                    });
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        }
    }
}

async fn serve_tls_metrics(
    listener: tokio::net::TcpListener,
    handle: PrometheusHandle,
    authority: BenchmarkAuthority,
    tls: Arc<rustls::ServerConfig>,
    shutdown: CancellationToken,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(connection) => connection,
                    Err(error) => {
                        tracing::debug!(%error, "benchmark authority metrics accept error");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let handle = handle.clone();
                let authority = authority.clone();
                tokio::spawn(async move {
                    let stream = match acceptor.accept(stream).await {
                        Ok(stream) => stream,
                        Err(error) => {
                            tracing::debug!(%peer, %error, "benchmark authority TLS handshake failed");
                            return;
                        }
                    };
                    let service = hyper::service::service_fn(move |request| {
                        std::future::ready(Ok::<_, std::convert::Infallible>(handle_request(
                            request,
                            &handle,
                            Some(&authority),
                        )))
                    });
                    let io = hyper_util::rt::TokioIo::new(stream);
                    if let Err(error) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await
                    {
                        tracing::debug!(%peer, %error, "benchmark authority HTTP connection failed");
                    }
                });
            }
        }
    }
}

fn collect_fs_stats(stats: &FileSystemStats) {
    counter!("zerofs_files_created_total").absolute(stats.files_created.load(Ordering::Relaxed));
    counter!("zerofs_files_deleted_total").absolute(stats.files_deleted.load(Ordering::Relaxed));
    counter!("zerofs_files_renamed_total").absolute(stats.files_renamed.load(Ordering::Relaxed));
    counter!("zerofs_directories_created_total")
        .absolute(stats.directories_created.load(Ordering::Relaxed));
    counter!("zerofs_directories_deleted_total")
        .absolute(stats.directories_deleted.load(Ordering::Relaxed));
    counter!("zerofs_directories_renamed_total")
        .absolute(stats.directories_renamed.load(Ordering::Relaxed));
    counter!("zerofs_links_created_total").absolute(stats.links_created.load(Ordering::Relaxed));
    counter!("zerofs_links_deleted_total").absolute(stats.links_deleted.load(Ordering::Relaxed));
    counter!("zerofs_links_renamed_total").absolute(stats.links_renamed.load(Ordering::Relaxed));
    counter!("zerofs_read_operations_total")
        .absolute(stats.read_operations.load(Ordering::Relaxed));
    counter!("zerofs_write_operations_total")
        .absolute(stats.write_operations.load(Ordering::Relaxed));
    counter!("zerofs_bytes_read_total").absolute(stats.bytes_read.load(Ordering::Relaxed));
    counter!("zerofs_bytes_written_total").absolute(stats.bytes_written.load(Ordering::Relaxed));
    counter!("zerofs_tombstones_created_total")
        .absolute(stats.tombstones_created.load(Ordering::Relaxed));
    counter!("zerofs_tombstones_processed_total")
        .absolute(stats.tombstones_processed.load(Ordering::Relaxed));
    counter!("zerofs_gc_extents_deleted_total")
        .absolute(stats.gc_extents_deleted.load(Ordering::Relaxed));
    counter!("zerofs_gc_runs_total").absolute(stats.gc_runs.load(Ordering::Relaxed));
    counter!("zerofs_total_operations").absolute(stats.total_operations.load(Ordering::Relaxed));
}

fn collect_global_stats(stats: &FileSystemGlobalStats) {
    let (used_bytes, used_inodes) = stats.get_totals();
    gauge!("zerofs_used_bytes").set(used_bytes as f64);
    gauge!("zerofs_used_inodes").set(used_inodes as f64);
}

fn collect_dedup_stats(dedup: &DedupCache) {
    let stats = dedup.stats();
    gauge!("zerofs_dedup_retained_results").set(stats.retained_results as f64);
    gauge!("zerofs_dedup_inflight_ids").set(stats.inflight_ids as f64);
    gauge!("zerofs_dedup_replay_pinned_results").set(stats.replay_pinned_results as f64);
}

fn collect_segment_gc_stats(stats: &SegmentGcStats) {
    let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);

    gauge!("zerofs_segment_gc_active").set(f64::from(stats.active.load(Ordering::Relaxed)));
    counter!("zerofs_segment_gc_passes_total").absolute(load(&stats.passes));
    counter!("zerofs_segment_gc_segments_deleted_total").absolute(load(&stats.segments_deleted));
    counter!("zerofs_segment_gc_deleted_bytes_total").absolute(load(&stats.deleted_bytes));
    counter!("zerofs_segment_gc_segments_compacted_total")
        .absolute(load(&stats.segments_compacted));
    counter!("zerofs_segment_gc_segments_packed_total").absolute(load(&stats.segments_packed));
    counter!("zerofs_segment_gc_frames_relocated_total").absolute(load(&stats.frames_relocated));
    counter!("zerofs_segment_gc_compaction_freed_bytes_total")
        .absolute(load(&stats.compaction_freed_bytes));
    counter!("zerofs_segment_gc_batches_total").absolute(load(&stats.batches));
    counter!("zerofs_segment_gc_tail_scrubbed_total").absolute(load(&stats.tail_scrubbed));
    counter!("zerofs_segment_gc_chains_packed_total").absolute(load(&stats.chains_packed));
    counter!("zerofs_segment_gc_nominations_total").absolute(load(&stats.nominations));
    counter!("zerofs_segment_gc_nominations_dropped_total")
        .absolute(load(&stats.nominations_dropped));
    counter!("zerofs_segment_gc_hot_seams_total").absolute(load(&stats.hot_seams));
    counter!("zerofs_segment_gc_orphans_reclaimed_total").absolute(load(&stats.orphans_reclaimed));

    let appended = load(&stats.appended_bytes);
    let reclaimable = load(&stats.reclaimable_bytes);
    gauge!("zerofs_segment_count").set(load(&stats.segment_count) as f64);
    gauge!("zerofs_segment_appended_bytes").set(appended as f64);
    gauge!("zerofs_segment_live_bytes").set(load(&stats.live_bytes) as f64);
    gauge!("zerofs_segment_reclaimable_bytes").set(reclaimable as f64);
    gauge!("zerofs_segment_dead_ratio").set(if appended > 0 {
        reclaimable as f64 / appended as f64
    } else {
        0.0
    });
    gauge!("zerofs_segment_gc_awaiting_delete").set(load(&stats.awaiting_delete) as f64);
    gauge!("zerofs_segment_gc_awaiting_delete_bytes")
        .set(load(&stats.awaiting_delete_bytes) as f64);
    gauge!("zerofs_segment_gc_candidate_backlog").set(load(&stats.candidate_backlog) as f64);
    gauge!("zerofs_segment_gc_chains_deferred").set(load(&stats.chains_deferred) as f64);
    gauge!("zerofs_segment_gc_saturated").set(load(&stats.saturated) as f64);
}

fn collect_jemalloc_stats() {
    let mem = crate::rpc::server::JemallocMemStats::read();
    gauge!("zerofs_jemalloc_allocated_bytes").set(mem.allocated as f64);
    gauge!("zerofs_jemalloc_resident_bytes").set(mem.resident as f64);
    gauge!("zerofs_jemalloc_mapped_bytes").set(mem.mapped as f64);
    gauge!("zerofs_jemalloc_retained_bytes").set(mem.retained as f64);
    gauge!("zerofs_jemalloc_metadata_bytes").set(mem.metadata as f64);
}

fn record_cache_metrics(snapshot: &CacheMetricsSnapshot) {
    fn record_tier(cache: &'static str, tier: &CacheTierSnapshot) {
        let count = |value: usize| u64::try_from(value).unwrap_or(u64::MAX);
        gauge!("zerofs_cache_logical_usage_bytes", "cache" => cache)
            .set(tier.logical_usage_bytes as f64);
        gauge!("zerofs_cache_logical_capacity_bytes", "cache" => cache)
            .set(tier.logical_capacity_bytes as f64);
        gauge!("zerofs_cache_entries", "cache" => cache).set(tier.entries as f64);
        counter!("zerofs_cache_disk_read_bytes_total", "cache" => cache)
            .absolute(count(tier.disk_read_bytes));
        counter!("zerofs_cache_disk_write_bytes_total", "cache" => cache)
            .absolute(count(tier.disk_write_bytes));
        counter!("zerofs_cache_disk_read_ios_total", "cache" => cache)
            .absolute(count(tier.disk_read_ios));
        counter!("zerofs_cache_disk_write_ios_total", "cache" => cache)
            .absolute(count(tier.disk_write_ios));
        counter!("zerofs_cache_queue_buffer_overflow_total", "cache" => cache)
            .absolute(tier.queue_buffer_overflow_total);
        counter!("zerofs_cache_queue_channel_overflow_total", "cache" => cache)
            .absolute(tier.queue_channel_overflow_total);
    }

    record_tier("raw_parts", &snapshot.raw_parts);
    record_tier("decoded_blocks", &snapshot.decoded_blocks);
}

fn collect_cache_metrics(cache_metrics: &CacheMetrics) {
    record_cache_metrics(&cache_metrics.snapshot());
}

fn collect_writeback_stats(writeback: Option<&WritebackObjectStore>) {
    gauge!("zerofs_writeback_enabled").set(f64::from(writeback.is_some()));
    let Some(writeback) = writeback else {
        gauge!("zerofs_writeback_status_collection_error").set(0.0);
        return;
    };
    match writeback.status() {
        Ok(status) => {
            gauge!("zerofs_writeback_status_collection_error").set(0.0);
            record_writeback_status(&status);
        }
        Err(error) => {
            gauge!("zerofs_writeback_status_collection_error").set(1.0);
            tracing::warn!(error = %error, "failed to collect writeback status");
        }
    }
}

fn record_writeback_status(status: &WritebackStatus) {
    gauge!("zerofs_writeback_dirty_ram_bytes").set(status.dirty_ram_bytes as f64);
    gauge!("zerofs_writeback_dirty_ram_capacity_bytes").set(status.dirty_ram_capacity_bytes as f64);
    gauge!("zerofs_writeback_dirty_ram_operations").set(status.dirty_ram_operations as f64);
    gauge!("zerofs_writeback_dirty_ssd_reserved_bytes").set(status.dirty_ssd_reserved_bytes as f64);
    gauge!("zerofs_writeback_dirty_ssd_capacity_bytes").set(status.dirty_ssd_capacity_bytes as f64);
    gauge!("zerofs_writeback_dirty_ssd_operations").set(status.dirty_ssd_operations as f64);
    gauge!("zerofs_writeback_accepted_sequence").set(status.accepted_seq as f64);
    gauge!("zerofs_writeback_local_sequence").set(status.local_seq as f64);
    gauge!("zerofs_writeback_remote_sequence").set(status.remote_seq as f64);
    gauge!("zerofs_writeback_local_lag_operations")
        .set(status.accepted_seq.saturating_sub(status.local_seq) as f64);
    gauge!("zerofs_writeback_remote_lag_operations")
        .set(status.accepted_seq.saturating_sub(status.remote_seq) as f64);
    gauge!("zerofs_writeback_ssd_remote_lag_operations")
        .set(status.local_seq.saturating_sub(status.remote_seq) as f64);
    gauge!("zerofs_writeback_oldest_pending_age_seconds")
        .set(status.oldest_pending_age_ms as f64 / 1_000.0);
    counter!("zerofs_writeback_local_bytes_completed_total").absolute(status.local_bytes_completed);
    counter!("zerofs_writeback_remote_bytes_completed_total")
        .absolute(status.remote_bytes_completed);
    counter!("zerofs_writeback_remote_operations_completed_total")
        .absolute(status.remote_operations_completed);
    counter!("zerofs_writeback_retries_total").absolute(status.retries);
    gauge!("zerofs_writeback_terminal_error").set(f64::from(status.terminal_error.is_some()));
}

/// Export name for a metadata-engine metric: the engine registers under
/// "slatedb.…", but the exported series speak the same vocabulary as the docs
/// and logs (the metadata LSM), so the prefix becomes "lsm_".
fn lsm_export_name(name: &str) -> String {
    format!(
        "lsm_{}",
        name.strip_prefix("slatedb.")
            .unwrap_or(name)
            .replace('.', "_")
    )
}

fn collect_lsm_stats(recorder: &DefaultMetricsRecorder) {
    let snapshot = recorder.snapshot();
    for metric in snapshot.all() {
        let prom_name = lsm_export_name(&metric.name);
        match &metric.value {
            MetricValue::Counter(v) => {
                counter!(prom_name).absolute(*v);
            }
            MetricValue::Gauge(v) => {
                gauge!(prom_name).set(*v as f64);
            }
            MetricValue::UpDownCounter(v) => {
                gauge!(prom_name).set(*v as f64);
            }
            MetricValue::Histogram { sum, .. } => {
                gauge!(prom_name).set(*sum);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BenchmarkAuthority, WRITEBACK_COLLECT_INTERVAL, lsm_export_name, record_writeback_status,
    };
    use crate::cache_metrics::{
        CacheMetrics, CacheMetricsSnapshot, CacheTierSnapshot, FoyerMetricsRegistry,
        build_test_cache,
    };
    use crate::config::{BenchmarkAdapter, BenchmarkAuthorityConfig, PrometheusConfig};
    use crate::writeback::model::WritebackStatus;
    use std::sync::Arc;

    #[test]
    fn writeback_metrics_refresh_fast_enough_for_durability_tier_measurement() {
        assert_eq!(
            WRITEBACK_COLLECT_INTERVAL,
            std::time::Duration::from_millis(100)
        );
    }

    #[test]
    fn benchmark_authority_identity_uses_valid_systemd_invocation_id() {
        let filesystem_id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let authority = BenchmarkAuthority::compose(
            "10.10.10.30:/",
            filesystem_id,
            Some("2be254ef917b4ff8a2c547b873709aef"),
        )
        .unwrap();

        assert_eq!(
            authority.server_instance_id,
            "2be254ef917b4ff8a2c547b873709aef"
        );
        assert_eq!(authority.filesystem_id, filesystem_id.to_string());
        assert_eq!(authority.export_id, "10.10.10.30:/");
    }

    #[test]
    fn benchmark_authority_identity_rejects_invalid_present_invocation_id() {
        let error = BenchmarkAuthority::compose(
            "10.10.10.30:/",
            uuid::Uuid::nil(),
            Some("invocation id with spaces"),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("server_instance_id"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn benchmark_authority_identity_fallback_changes_between_process_compositions() {
        let filesystem_id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let first = BenchmarkAuthority::compose("10.10.10.30:/", filesystem_id, None).unwrap();
        let second = BenchmarkAuthority::compose("10.10.10.30:/", filesystem_id, None).unwrap();

        assert_ne!(first.server_instance_id, second.server_instance_id);
        assert_eq!(first.filesystem_id, second.filesystem_id);
        assert_eq!(first.export_id, second.export_id);
    }

    fn benchmark_authority_fixture() -> BenchmarkAuthority {
        BenchmarkAuthority::compose(
            "10.10.10.30:/",
            uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            Some("2be254ef917b4ff8a2c547b873709aef"),
        )
        .unwrap()
    }

    fn sample_count(text: &str, metric: &str) -> usize {
        text.lines()
            .filter(|line| {
                !line.starts_with('#')
                    && line
                        .split_once(['{', ' '])
                        .is_some_and(|(name, _)| name == metric)
            })
            .count()
    }

    #[test]
    fn benchmark_authority_response_emits_exactly_one_tuple_without_relabeling_metrics() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("zerofs_bytes_read_total").absolute(7);
        });
        let authority = benchmark_authority_fixture();

        let first = super::render_metrics(&handle, Some(&authority));
        let second = super::render_metrics(&handle, Some(&authority));

        let expected = concat!(
            "zerofs_benchmark_authority_info{",
            "server_instance_id=\"2be254ef917b4ff8a2c547b873709aef\",",
            "filesystem_id=\"550e8400-e29b-41d4-a716-446655440000\",",
            "export_id=\"10.10.10.30:/\"} 1"
        );
        for body in [&first, &second] {
            assert_eq!(sample_count(body, "zerofs_benchmark_authority_info"), 1);
            assert!(body.lines().any(|line| line == expected), "body:\n{body}");
            assert!(body.contains("zerofs_bytes_read_total 7"), "body:\n{body}");
            assert!(!body.contains("zerofs_bytes_read_total{"), "body:\n{body}");
        }
    }

    #[tokio::test]
    async fn benchmark_authority_response_accepts_only_canonical_unauthenticated_get() {
        use http_body_util::BodyExt;

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let authority = benchmark_authority_fixture();
        let canonical = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri("/metrics")
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .unwrap();
        let response = super::handle_request(canonical, &handle, Some(&authority));
        assert_eq!(response.status(), hyper::StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            sample_count(
                std::str::from_utf8(&body).unwrap(),
                "zerofs_benchmark_authority_info"
            ),
            1
        );

        for request in [
            hyper::Request::builder()
                .method(hyper::Method::POST)
                .uri("/metrics"),
            hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri("/metrics?x=1"),
            hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri("/metrics/"),
            hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri("/"),
            hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri("/metrics")
                .header(hyper::header::AUTHORIZATION, "Bearer secret"),
        ] {
            let response = super::handle_request(
                request
                    .body(http_body_util::Empty::<bytes::Bytes>::new())
                    .unwrap(),
                &handle,
                Some(&authority),
            );
            assert_ne!(response.status(), hyper::StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                sample_count(
                    std::str::from_utf8(&body).unwrap(),
                    "zerofs_benchmark_authority_info"
                ),
                0
            );
        }
    }

    #[test]
    fn benchmark_authority_response_disabled_preserves_legacy_path_behavior() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let legacy_post = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri("/metrics")
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .unwrap();

        let response = super::handle_request(legacy_post, &handle, None);

        assert_eq!(response.status(), hyper::StatusCode::OK);
    }

    #[cfg(unix)]
    fn generate_benchmark_authority_tls_material() -> (String, String) {
        let rcgen::CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
        ])
        .unwrap();
        (cert.pem(), signing_key.serialize_pem())
    }

    #[cfg(unix)]
    fn benchmark_authority_tls_fixture(
        certificate: &str,
        private_key: &str,
        private_key_mode: u32,
    ) -> (tempfile::TempDir, BenchmarkAuthorityConfig) {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let certificate_path = directory.path().join("metrics.crt");
        let private_key_path = directory.path().join("metrics.key");
        std::fs::write(&certificate_path, certificate).unwrap();
        std::fs::write(&private_key_path, private_key).unwrap();
        std::fs::set_permissions(
            &private_key_path,
            std::fs::Permissions::from_mode(private_key_mode),
        )
        .unwrap();
        (
            directory,
            BenchmarkAuthorityConfig {
                adapter: BenchmarkAdapter::Nfs,
                export_id: "127.0.0.1:/".to_owned(),
                tls_certificate: certificate_path,
                tls_private_key: private_key_path,
            },
        )
    }

    #[cfg(unix)]
    #[test]
    fn benchmark_authority_tls_loads_matching_secure_material() {
        let (certificate, private_key) = generate_benchmark_authority_tls_material();
        let (_directory, config) =
            benchmark_authority_tls_fixture(&certificate, &private_key, 0o600);

        super::load_tls_config(&config).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn benchmark_authority_tls_rejects_insecure_or_invalid_material() {
        let (certificate, private_key) = generate_benchmark_authority_tls_material();
        let (_directory, config) =
            benchmark_authority_tls_fixture(&certificate, &private_key, 0o644);
        let error = super::load_tls_config(&config).expect_err("world-readable key");
        assert!(
            format!("{error:#}").contains("private key permissions"),
            "unexpected permissions error: {error:#}"
        );

        let (_directory, config) = benchmark_authority_tls_fixture("", &private_key, 0o600);
        let error = super::load_tls_config(&config).expect_err("empty certificate");
        assert!(
            format!("{error:#}").contains("certificate chain"),
            "unexpected certificate error: {error:#}"
        );

        let (_, other_private_key) = generate_benchmark_authority_tls_material();
        let (_directory, config) =
            benchmark_authority_tls_fixture(&certificate, &other_private_key, 0o600);
        let error = super::load_tls_config(&config).expect_err("mismatched private key");
        assert!(
            format!("{error:#}").contains("does not match"),
            "unexpected mismatch error: {error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn benchmark_authority_tls_listener_serves_https_and_refuses_plaintext() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (certificate_pem, private_key) = generate_benchmark_authority_tls_material();
        let (_directory, config) =
            benchmark_authority_tls_fixture(&certificate_pem, &private_key, 0o600);
        let tls = super::load_tls_config(&config).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let authority = benchmark_authority_fixture();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(super::serve_tls_metrics(
            listener,
            handle,
            authority,
            tls,
            server_shutdown,
        ));

        let certificate = reqwest::tls::Certificate::from_pem(certificate_pem.as_bytes()).unwrap();
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .add_root_certificate(certificate)
            .build()
            .unwrap();
        let response = client
            .get(format!("https://127.0.0.1:{}/metrics", address.port()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body = response.text().await.unwrap();
        assert_eq!(sample_count(&body, "zerofs_benchmark_authority_info"), 1);

        let mut plaintext = tokio::net::TcpStream::connect(address).await.unwrap();
        plaintext
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        plaintext.shutdown().await.unwrap();
        let mut received = Vec::new();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            plaintext.read_to_end(&mut received),
        )
        .await;
        assert!(
            !received
                .windows(b"zerofs_benchmark_authority_info".len())
                .any(|window| window == b"zerofs_benchmark_authority_info"),
            "plaintext request exposed metrics"
        );

        shutdown.cancel();
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn benchmark_authority_startup_requires_identity_and_prebinds_tls_address() {
        let (certificate, private_key) = generate_benchmark_authority_tls_material();
        let (_directory, authority_config) =
            benchmark_authority_tls_fixture(&certificate, &private_key, 0o600);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let config = PrometheusConfig {
            addresses: std::iter::once(address).collect(),
            benchmark_authority: Some(authority_config),
        };

        let missing = match super::prepare_authority_server(&config, None).await {
            Err(error) => error,
            Ok(_) => panic!("missing authority identity must fail startup"),
        };
        assert!(
            format!("{missing:#}").contains("identity is missing"),
            "unexpected error: {missing:#}"
        );

        let prepared =
            super::prepare_authority_server(&config, Some(benchmark_authority_fixture()))
                .await
                .unwrap()
                .expect("authority mode must prepare a TLS listener");
        assert_eq!(prepared.listeners.len(), 1);
        assert_eq!(prepared.listeners[0].local_addr().unwrap(), address);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn benchmark_authority_startup_fails_when_tls_address_is_occupied() {
        let (certificate, private_key) = generate_benchmark_authority_tls_material();
        let (_directory, authority_config) =
            benchmark_authority_tls_fixture(&certificate, &private_key, 0o600);
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = PrometheusConfig {
            addresses: std::iter::once(occupied.local_addr().unwrap()).collect(),
            benchmark_authority: Some(authority_config),
        };

        let error =
            match super::prepare_authority_server(&config, Some(benchmark_authority_fixture()))
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("occupied authority address must fail startup"),
            };

        assert!(
            format!("{error:#}").contains("failed to bind benchmark authority metrics"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn benchmark_authority_startup_loads_durable_filesystem_identity() {
        use slatedb::object_store::{ObjectStore, ObjectStoreExt, path::Path};

        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let filesystem_id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        store
            .put(
                &Path::from("data").join(".zerofs_bucket_id"),
                filesystem_id.to_string().into(),
            )
            .await
            .unwrap();

        let authority = BenchmarkAuthority::load(
            "10.10.10.30:/",
            &store,
            "data",
            Some("2be254ef917b4ff8a2c547b873709aef"),
        )
        .await
        .unwrap();

        assert_eq!(authority.filesystem_id, filesystem_id.to_string());
        assert_eq!(
            authority.server_instance_id,
            "2be254ef917b4ff8a2c547b873709aef"
        );
    }

    #[tokio::test]
    async fn legacy_metrics_startup_fails_when_plaintext_address_is_occupied() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = PrometheusConfig {
            addresses: std::iter::once(occupied.local_addr().unwrap()).collect(),
            benchmark_authority: None,
        };

        let error = match super::bind_plaintext_listeners(&config).await {
            Err(error) => error,
            Ok(_) => panic!("occupied plaintext metrics address must fail startup"),
        };

        assert!(
            format!("{error:#}").contains("failed to bind Prometheus metrics"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn engine_metric_names_export_under_the_lsm_prefix() {
        assert_eq!(
            lsm_export_name("slatedb.compactor.bytes_compacted"),
            "lsm_compactor_bytes_compacted"
        );
        // A name without the engine prefix still exports under lsm_: the
        // recorder holds only metadata-engine metrics.
        assert_eq!(lsm_export_name("some.other.stat"), "lsm_some_other_stat");
    }

    #[test]
    fn writeback_metrics_expose_independent_dirty_ram_and_ssd_budgets() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::set_global_recorder(recorder).unwrap();
        let status = WritebackStatus {
            accepted_seq: 9,
            local_seq: 8,
            remote_seq: 5,
            dirty_ram_bytes: 4,
            dirty_ram_capacity_bytes: 16,
            dirty_ram_operations: 1,
            dirty_ssd_reserved_bytes: 3,
            dirty_ssd_capacity_bytes: 512,
            dirty_ssd_operations: 2,
            oldest_pending_age_ms: 6_000,
            local_bytes_completed: 11,
            remote_bytes_completed: 7,
            remote_operations_completed: 5,
            retries: 2,
            terminal_error: Some("remote unavailable".to_owned()),
        };

        record_writeback_status(&status);
        let rendered = handle.render();
        for expected in [
            "zerofs_writeback_dirty_ram_bytes 4",
            "zerofs_writeback_dirty_ram_capacity_bytes 16",
            "zerofs_writeback_dirty_ssd_reserved_bytes 3",
            "zerofs_writeback_dirty_ssd_capacity_bytes 512",
            "zerofs_writeback_remote_lag_operations 4",
            "zerofs_writeback_ssd_remote_lag_operations 3",
            "zerofs_writeback_oldest_pending_age_seconds 6",
            "zerofs_writeback_local_bytes_completed_total 11",
            "zerofs_writeback_remote_bytes_completed_total 7",
            "zerofs_writeback_retries_total 2",
            "zerofs_writeback_terminal_error 1",
        ] {
            assert!(
                rendered.contains(expected),
                "missing metric: {expected}\n{rendered}"
            );
        }
    }

    #[test]
    fn cache_metrics_export_fixed_low_cardinality_tiers() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let snapshot = CacheMetricsSnapshot {
            raw_parts: CacheTierSnapshot {
                logical_usage_bytes: 11,
                logical_capacity_bytes: 22,
                entries: 3,
                disk_read_bytes: 44,
                disk_write_bytes: 55,
                disk_read_ios: 6,
                disk_write_ios: 7,
                queue_buffer_overflow_total: 8,
                queue_channel_overflow_total: 9,
            },
            decoded_blocks: CacheTierSnapshot {
                logical_usage_bytes: 111,
                logical_capacity_bytes: 222,
                entries: 33,
                disk_read_bytes: 444,
                disk_write_bytes: 555,
                disk_read_ios: 66,
                disk_write_ios: 77,
                queue_buffer_overflow_total: 88,
                queue_channel_overflow_total: 99,
            },
        };

        metrics::with_local_recorder(&recorder, || super::record_cache_metrics(&snapshot));
        let rendered = handle.render();
        for expected in [
            "zerofs_cache_logical_usage_bytes{cache=\"raw_parts\"} 11",
            "zerofs_cache_logical_capacity_bytes{cache=\"raw_parts\"} 22",
            "zerofs_cache_entries{cache=\"raw_parts\"} 3",
            "zerofs_cache_disk_read_bytes_total{cache=\"raw_parts\"} 44",
            "zerofs_cache_disk_write_bytes_total{cache=\"raw_parts\"} 55",
            "zerofs_cache_disk_read_ios_total{cache=\"raw_parts\"} 6",
            "zerofs_cache_disk_write_ios_total{cache=\"raw_parts\"} 7",
            "zerofs_cache_queue_buffer_overflow_total{cache=\"raw_parts\"} 8",
            "zerofs_cache_queue_channel_overflow_total{cache=\"raw_parts\"} 9",
            "zerofs_cache_logical_usage_bytes{cache=\"decoded_blocks\"} 111",
            "zerofs_cache_logical_capacity_bytes{cache=\"decoded_blocks\"} 222",
            "zerofs_cache_entries{cache=\"decoded_blocks\"} 33",
            "zerofs_cache_disk_read_bytes_total{cache=\"decoded_blocks\"} 444",
            "zerofs_cache_disk_write_bytes_total{cache=\"decoded_blocks\"} 555",
            "zerofs_cache_disk_read_ios_total{cache=\"decoded_blocks\"} 66",
            "zerofs_cache_disk_write_ios_total{cache=\"decoded_blocks\"} 77",
            "zerofs_cache_queue_buffer_overflow_total{cache=\"decoded_blocks\"} 88",
            "zerofs_cache_queue_channel_overflow_total{cache=\"decoded_blocks\"} 99",
        ] {
            assert!(
                rendered.contains(expected),
                "missing metric: {expected}\n{rendered}"
            );
        }
    }

    #[tokio::test]
    async fn collector_tick_snapshots_both_live_cache_tiers() {
        const KIB: usize = 1024;

        let dir = tempfile::tempdir().unwrap();
        let registry = FoyerMetricsRegistry::default();
        let raw_parts = build_test_cache(
            &dir.path().join("raw-parts"),
            "zerofs-object-prefetch-parts",
            32 * KIB,
            16 * KIB,
            registry.clone(),
        )
        .await;
        let decoded_blocks = build_test_cache(
            &dir.path().join("decoded-blocks"),
            "zerofs-slatedb-hybrid",
            16 * KIB,
            16 * KIB,
            registry.clone(),
        )
        .await;
        raw_parts.insert(1, vec![1; 4 * KIB]);
        decoded_blocks.insert(1, vec![2; 2 * KIB]);
        let cache_metrics = CacheMetrics::new(raw_parts.clone(), decoded_blocks.clone(), registry);
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || super::collect_cache_metrics(&cache_metrics));

        let rendered = handle.render();
        for expected in [
            "zerofs_cache_logical_usage_bytes{cache=\"raw_parts\"} 4096",
            "zerofs_cache_logical_capacity_bytes{cache=\"raw_parts\"} 32768",
            "zerofs_cache_entries{cache=\"raw_parts\"} 1",
            "zerofs_cache_logical_usage_bytes{cache=\"decoded_blocks\"} 2048",
            "zerofs_cache_logical_capacity_bytes{cache=\"decoded_blocks\"} 16384",
            "zerofs_cache_entries{cache=\"decoded_blocks\"} 1",
        ] {
            assert!(
                rendered.contains(expected),
                "missing metric: {expected}\n{rendered}"
            );
        }

        raw_parts.close().await.unwrap();
        decoded_blocks.close().await.unwrap();
    }
}
