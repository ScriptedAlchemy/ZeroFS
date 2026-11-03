//! Client-side SFTP wait and READ-future observations. These measure local
//! future lifetimes, not TCP packets or server-side outstanding requests.
//! Static labels keep metric cardinality independent of paths and reconnects.

use crate::sftp_transport::OperationKind;
use metrics::Gauge;
use std::future::Future;
use tokio::time::Instant;

#[derive(Clone, Copy)]
pub(crate) enum SftpPhase {
    Admission,
    Session,
    Open,
    Fstat,
    Header,
    Payload,
    Close,
}

impl SftpPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Session => "session",
            Self::Open => "open",
            Self::Fstat => "fstat",
            Self::Header => "header",
            Self::Payload => "payload",
            Self::Close => "close",
        }
    }
}

struct PhaseObservation {
    started: Instant,
    phase: &'static str,
    kind: &'static str,
    outcome: &'static str,
    inflight: Gauge,
}

impl Drop for PhaseObservation {
    fn drop(&mut self) {
        self.inflight.decrement(1.0);
        metrics::histogram!(
            "zerofs_sftp_phase_duration_seconds",
            "phase" => self.phase, "kind" => self.kind, "outcome" => self.outcome,
        )
        .record(self.started.elapsed().as_secs_f64());
    }
}

/// Observe only once this future is polled. Dropping a pending phase releases
/// its gauge and records cancellation without changing its underlying result.
pub(crate) async fn observe_phase<F, T, E>(
    phase: SftpPhase,
    kind: OperationKind,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let phase = phase.label();
    let kind = match kind {
        OperationKind::Read => "read",
        OperationKind::Write => "write",
        OperationKind::Metadata => "metadata",
    };
    let inflight = metrics::gauge!("zerofs_sftp_phase_inflight", "phase" => phase, "kind" => kind);
    inflight.increment(1.0);
    let mut observation = PhaseObservation {
        started: Instant::now(),
        phase,
        kind,
        outcome: "cancelled",
        inflight,
    };
    let result = future.await;
    observation.outcome = if result.is_ok() { "success" } else { "error" };
    result
}

pub(crate) struct ReadRequestObservation {
    started: Instant,
    requested: usize,
    outcome: &'static str,
    inflight: Gauge,
    bytes_inflight: Gauge,
}

impl ReadRequestObservation {
    /// Call inside the polled request future, not when constructing the queue.
    pub(crate) fn start(requested: usize) -> Self {
        let inflight = metrics::gauge!("zerofs_sftp_read_requests_inflight");
        let bytes_inflight = metrics::gauge!("zerofs_sftp_read_requested_bytes_inflight");
        inflight.increment(1.0);
        bytes_inflight.increment(requested as f64);
        metrics::counter!("zerofs_sftp_read_requests_total").increment(1);
        metrics::counter!("zerofs_sftp_read_requested_bytes_total").increment(requested as u64);
        Self {
            started: Instant::now(),
            requested,
            outcome: "cancelled",
            inflight,
            bytes_inflight,
        }
    }

    pub(crate) fn data(&mut self, received: usize) {
        self.outcome = "data";
        // Count every DATA reply, even when a later error discards the object.
        // The existing object-read counter only counts successful payloads.
        metrics::counter!("zerofs_sftp_read_reply_bytes_total").increment(received as u64);
        metrics::histogram!("zerofs_sftp_read_reply_size_bytes").record(received as f64);
        if received > 0 && received < self.requested {
            metrics::counter!("zerofs_sftp_read_short_replies_total").increment(1);
        }
    }

    pub(crate) fn eof(&mut self) {
        self.outcome = "eof";
    }

    pub(crate) fn error(&mut self) {
        self.outcome = "error";
    }
}

impl Drop for ReadRequestObservation {
    fn drop(&mut self) {
        self.inflight.decrement(1.0);
        self.bytes_inflight.decrement(self.requested as f64);
        metrics::counter!("zerofs_sftp_read_requests_finished_total", "outcome" => self.outcome)
            .increment(1);
        metrics::histogram!("zerofs_sftp_read_request_duration_seconds", "outcome" => self.outcome)
            .record(self.started.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::time::Duration;

    pub(crate) fn metric_value(body: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
        body.lines()
            .filter(|line| !line.starts_with('#'))
            .find_map(|line| {
                let (key, value) = line.rsplit_once(' ')?;
                let metric_name = key.split('{').next()?;
                (metric_name == name
                    && labels
                        .iter()
                        .all(|(k, v)| key.contains(&format!("{k}=\"{v}\""))))
                .then(|| value.parse::<f64>().unwrap())
            })
            .unwrap_or_else(|| panic!("missing metric {name} {labels:?} in {body}"))
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn phase_metrics_preserve_results_and_release_cancelled_waits() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let pending = observe_phase(
            SftpPhase::Admission,
            OperationKind::Read,
            std::future::pending::<Result<(), ()>>(),
        );
        let mut pending = Box::pin(pending);
        assert!(futures::poll!(pending.as_mut()).is_pending());
        let labels = &[("phase", "admission"), ("kind", "read")];
        assert_eq!(
            metric_value(&handle.render(), "zerofs_sftp_phase_inflight", labels),
            1.0
        );
        tokio::time::advance(Duration::from_millis(125)).await;
        drop(pending);
        let body = handle.render();
        assert_eq!(
            metric_value(&body, "zerofs_sftp_phase_inflight", labels),
            0.0
        );
        assert_eq!(
            metric_value(
                &body,
                "zerofs_sftp_phase_duration_seconds_sum",
                &[
                    ("phase", "admission"),
                    ("kind", "read"),
                    ("outcome", "cancelled")
                ]
            ),
            0.125
        );

        let result = observe_phase(SftpPhase::Open, OperationKind::Read, async {
            Ok::<_, &str>(42)
        })
        .await;
        assert_eq!(result, Ok(42));
        let result = observe_phase(SftpPhase::Close, OperationKind::Read, async {
            Err::<(), _>("close failed")
        })
        .await;
        assert_eq!(result, Err("close failed"));
        let body = handle.render();
        for (phase, outcome) in [("open", "success"), ("close", "error")] {
            assert_eq!(
                metric_value(
                    &body,
                    "zerofs_sftp_phase_inflight",
                    &[("phase", phase), ("kind", "read")]
                ),
                0.0
            );
            assert_eq!(
                metric_value(
                    &body,
                    "zerofs_sftp_phase_duration_seconds_count",
                    &[("phase", phase), ("kind", "read"), ("outcome", outcome)]
                ),
                1.0
            );
        }
    }
}
