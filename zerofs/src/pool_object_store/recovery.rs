use super::PoolObjectStore;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct PoolRecovery {
    stop: CancellationToken,
    task: JoinHandle<object_store::Result<()>>,
}

impl PoolRecovery {
    pub(crate) fn start(pool: PoolObjectStore, failure: CancellationToken) -> Self {
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let task = tokio::spawn(async move {
            let _failure = failure.drop_guard();
            loop {
                if cancelled.is_cancelled() {
                    return Ok(());
                }
                pool.evacuate_pass(&cancelled).await?;
                pool.recover_pending().await?;
                for fault in pool
                    .faults()
                    .await?
                    .into_iter()
                    .filter(|f| f.next_retry_ms <= chrono::Utc::now().timestamp_millis())
                    .take(16)
                {
                    if cancelled.is_cancelled() {
                        break;
                    }
                    pool.verify_source_fault(&fault).await?;
                }
                tokio::select! {
                    _ = cancelled.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                    _ = pool.inner.health_changed.notified() => {}
                }
            }
        });
        Self { stop, task }
    }

    pub(crate) async fn shutdown(self) -> anyhow::Result<()> {
        self.stop.cancel();
        self.task.await??;
        Ok(())
    }
}

impl PoolObjectStore {
    pub(super) async fn verify_source_fault(
        &self,
        fault: &super::SegmentFault,
    ) -> object_store::Result<()> {
        let location = object_store::path::Path::parse(&fault.location)
            .map_err(|e| super::error(e.to_string()))?;
        let Ok(_guard) = self.key_lock(&location).try_write_owned() else {
            return Ok(());
        };
        let _owner = self.inner.tasks.token();
        let Some(entry) = self.visible(&location).await? else {
            return Ok(());
        };
        if entry.physical.key() != fault.physical_key {
            return Ok(());
        }
        let Some(expected) = entry.sha256 else {
            return Ok(());
        };
        match self.hash_physical(&entry.physical, entry.size).await {
            Ok(actual) if actual == expected => {
                let location = fault.location.clone();
                let generation = fault.physical_key.clone();
                self.catalog(move |c| c.clear_fault_if_current(&location, &generation))
                    .await?;
            }
            failure => {
                let reason = match failure {
                    Ok(_) => "source checksum still mismatches".to_owned(),
                    Err(e) => e.to_string(),
                };
                let location = fault.location.clone();
                self.catalog(move |c| c.record_source_fault(&location, &entry, reason))
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn evacuate_pass(&self, stop: &CancellationToken) -> object_store::Result<()> {
        let status = self.status().await?;
        let sick: BTreeSet<_> = status
            .members
            .iter()
            .filter(|m| m.health.quarantine.is_some())
            .map(|m| m.id.as_str())
            .collect();
        if sick.is_empty() {
            return Ok(());
        }
        let damaged: BTreeSet<_> = self
            .faults()
            .await?
            .into_iter()
            .filter(|f| f.next_retry_ms > chrono::Utc::now().timestamp_millis())
            .map(|f| f.location)
            .collect();
        let mut cursor = String::new();
        loop {
            let after = cursor.clone();
            let candidates = self.catalog(move |c| c.placement_page(&after)).await?;
            if candidates.is_empty() {
                break;
            }
            for placement in candidates {
                cursor = placement.location.to_string();
                if stop.is_cancelled() {
                    return Ok(());
                }
                if !sick.contains(placement.member_id.as_str())
                    || damaged.contains(placement.location.as_ref())
                {
                    continue;
                }
                for target in status.members.iter().filter(|m| {
                    m.health.quarantine.is_none() && m.health.monitoring_error.is_none()
                }) {
                    if stop.is_cancelled() {
                        break;
                    }
                    match self
                        .migrate_from(
                            &placement.location,
                            &target.id,
                            Some(placement.member_id.clone()),
                        )
                        .await
                    {
                        Ok(()) => break,
                        Err(super::MoveFailure::Control(error)) => return Err(error),
                        Err(error) => {
                            let issue = match &error {
                                super::MoveFailure::Admission(_) => {
                                    "capacity_or_allocation_blocked"
                                }
                                super::MoveFailure::Source(_) => "source_unreadable",
                                _ => "destination_unavailable",
                            }
                            .to_owned();
                            let member = placement.member_id.clone();
                            self.catalog(move |c| c.evacuation_error(&member, Some(issue)))
                                .await?;
                            tracing::warn!(%error, source = %placement.member_id, target = %target.id, "pool evacuation remains incomplete")
                        }
                    }
                    if self
                        .faults()
                        .await?
                        .iter()
                        .any(|f| f.location == placement.location.as_ref())
                    {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
