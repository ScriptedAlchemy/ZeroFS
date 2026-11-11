use super::{PoolMemberStatus, PoolObjectStore, PoolStatus};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

const MAX_FAULT_VERIFICATIONS_PER_PASS: usize = 16;
const MULTIPLE_SICK_MEMBERS: &str = "multiple_sick_members";

/// Historical quarantine on an already emptied member must not turn a later
/// single-device failure into a permanent multi-device stop. Count unfinished
/// zero-byte operations too: their physical ownership still has to settle.
fn active_failure(member: &PoolMemberStatus) -> bool {
    member.health.quarantine.is_some()
        && (member.objects != 0
            || member.used_bytes != 0
            || member.reserved_bytes != 0
            || member.pending_uploads != 0
            || member.cleanup_debts != 0)
}

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
                pool.verify_due_faults(&cancelled, chrono::Utc::now().timestamp_millis())
                    .await?;
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
    /// This cut deliberately has no policy for evacuating two failed devices.
    /// Leave their remaining placements in place, retain committed moves, and
    /// publish a durable explanation while ordinary debt cleanup can continue.
    async fn single_failure_status(&self) -> object_store::Result<Option<PoolStatus>> {
        let status = self.status().await?;
        let multiple = status.members.iter().filter(|m| active_failure(m)).count() > 1;
        for member in status.members.iter().filter(|m| active_failure(m)) {
            let previous = member.health.evacuation_error.as_deref();
            let issue = if multiple {
                Some(MULTIPLE_SICK_MEMBERS.to_owned())
            } else if previous == Some(MULTIPLE_SICK_MEMBERS) {
                None
            } else {
                continue;
            };
            if previous != issue.as_deref() {
                let id = member.id.clone();
                self.catalog(move |c| c.evacuation_error(&id, issue))
                    .await?;
            }
        }
        Ok((!multiple).then_some(status))
    }

    async fn verify_due_faults(
        &self,
        stop: &CancellationToken,
        now_ms: i64,
    ) -> object_store::Result<()> {
        let mut attempted = 0;
        for fault in self.faults().await? {
            if stop.is_cancelled() || attempted == MAX_FAULT_VERIFICATIONS_PER_PASS {
                break;
            }
            if fault.next_retry_ms > now_ms {
                continue;
            }
            // A busy lock (including a colliding key's read lease) must not
            // consume the budget and indefinitely starve later fault entries.
            if self.verify_source_fault(&fault).await? {
                attempted += 1;
            }
        }
        Ok(())
    }

    /// Returns whether physical verification was attempted, including failures.
    pub(super) async fn verify_source_fault(
        &self,
        fault: &super::SegmentFault,
    ) -> object_store::Result<bool> {
        let location = object_store::path::Path::parse(&fault.location)
            .map_err(|e| super::error(e.to_string()))?;
        let Ok(_guard) = self.key_lock(&location).try_write_owned() else {
            return Ok(false);
        };
        let _owner = self.inner.tasks.token();
        let Some(entry) = self.visible(&location).await? else {
            return Ok(false);
        };
        if entry.physical.key() != fault.physical_key {
            return Ok(false);
        }
        let Some(expected) = entry.sha256 else {
            return Ok(false);
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
        Ok(true)
    }

    pub(crate) async fn evacuate_pass(&self, stop: &CancellationToken) -> object_store::Result<()> {
        let Some(status) = self.single_failure_status().await? else {
            return Ok(());
        };
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
                        .migrate_automatic(
                            &placement.location,
                            &target.id,
                            placement.member_id.clone(),
                        )
                        .await
                    {
                        Ok(true) => {
                            let member = placement.member_id.clone();
                            self.catalog(move |c| c.evacuation_error(&member, None))
                                .await?;
                            break;
                        }
                        // A reader or another migration owns this object. Its
                        // lock is independent of the target; keep walking so a
                        // held read lease cannot starve later source segments.
                        Ok(false) => break,
                        Err(super::MoveFailure::Control(error)) => return Err(error),
                        Err(error) => {
                            // Admission/publication also check the policy in
                            // the catalog, so a second failure during a copy
                            // fences the remap and stops subsequent copies.
                            if self.single_failure_status().await?.is_none() {
                                return Ok(());
                            }
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
