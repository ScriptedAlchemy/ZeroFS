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
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
    pub slatedb_registry: Option<Arc<DefaultMetricsRecorder>>,
    pub writeback: Option<WritebackObjectStore>,
}

pub fn start(
    config: &PrometheusConfig,
    sources: CollectorSources,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    metrics::set_global_recorder(recorder).expect("failed to install Prometheus recorder");

    let mut handles = Vec::new();

    for &addr in &config.addresses {
        tracing::info!(
            "Prometheus metrics server listening on http://{}/metrics",
            addr
        );
        let server_handle = handle.clone();
        let server_shutdown = shutdown.clone();
        handles.push(spawn_named("prometheus-http", async move {
            serve_metrics(addr, server_handle, server_shutdown).await;
        }));
    }

    let upkeep_handle = handle.clone();
    handles.push(spawn_named("prometheus-collector", async move {
        let CollectorSources {
            fs_stats,
            global_stats,
            segment_gc_stats,
            dedup,
            slatedb_registry,
            writeback,
        } = sources;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Prometheus collector shutting down");
                    break;
                }
                _ = interval.tick() => {
                    collect_fs_stats(&fs_stats);
                    collect_global_stats(&global_stats);
                    collect_segment_gc_stats(&segment_gc_stats);
                    collect_dedup_stats(&dedup);
                    if let Some(ref registry) = slatedb_registry {
                        collect_lsm_stats(registry);
                    }
                    collect_writeback_stats(writeback.as_ref());
                    collect_jemalloc_stats();
                    upkeep_handle.run_upkeep();
                }
            }
        }
    }));

    handles
}

type HttpResponse = hyper::Response<http_body_util::Full<bytes::Bytes>>;

fn handle_request(
    req: hyper::Request<impl hyper::body::Body>,
    metrics_handle: &PrometheusHandle,
) -> HttpResponse {
    if req.uri().path() != "/metrics" {
        return hyper::Response::builder()
            .status(404)
            .body(http_body_util::Full::new(bytes::Bytes::from("Not Found")))
            .unwrap();
    }

    hyper::Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(http_body_util::Full::new(bytes::Bytes::from(
            metrics_handle.render(),
        )))
        .unwrap()
}

async fn serve_metrics(addr: SocketAddr, handle: PrometheusHandle, shutdown: CancellationToken) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind Prometheus HTTP server to {}: {}", addr, e);
            return;
        }
    };

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
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        std::future::ready(Ok::<_, std::convert::Infallible>(
                            handle_request(req, &handle),
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
    gauge!("zerofs_writeback_dirty_ssd_bytes").set(status.dirty_ssd_bytes as f64);
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
    use super::{lsm_export_name, record_writeback_status};
    use crate::writeback::model::WritebackStatus;

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
            dirty_ssd_bytes: 3,
            dirty_ssd_capacity_bytes: 512,
            dirty_ssd_operations: 2,
            oldest_pending_age_ms: 6_000,
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
            "zerofs_writeback_dirty_ssd_bytes 3",
            "zerofs_writeback_dirty_ssd_capacity_bytes 512",
            "zerofs_writeback_remote_lag_operations 4",
            "zerofs_writeback_ssd_remote_lag_operations 3",
            "zerofs_writeback_oldest_pending_age_seconds 6",
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
}
