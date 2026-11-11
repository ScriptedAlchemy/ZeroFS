//! Conservative evacuation snapshots using the allocator's capacity budgets.
use super::{PoolMember, PoolMemberStatus};
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Copy)]
struct Space {
    filesystem: u64,
    available: u64,
}
struct Budget {
    available: u64,
    reserve: u64,
    quota: u64,
}

pub(super) fn assess(config: &[PoolMember], members: &mut [PoolMemberStatus]) {
    assess_with(config, members, |member| {
        member.validate_binding()?;
        let path = member.local_path.as_ref().expect("local member");
        Ok(Space {
            filesystem: std::fs::metadata(path)?.dev(),
            available: fs4::available_space(path)?,
        })
    });
}

fn assess_with(
    config: &[PoolMember],
    members: &mut [PoolMemberStatus],
    mut space: impl FnMut(&PoolMember) -> std::io::Result<Space>,
) {
    // Match reserve(): unpublished local bytes may not yet appear in statvfs.
    let local_reserved = config
        .iter()
        .zip(members.iter())
        .filter(|(config, _)| config.local_path.is_some())
        .fold(0_u64, |sum, (_, m)| sum.saturating_add(m.reserved_bytes));
    let mut filesystems = BTreeMap::<u64, Budget>::new();
    let mut available = 0_u64;
    let mut unavailable = Vec::new();
    for (config, member) in config.iter().zip(members.iter_mut()) {
        member.available_bytes = 0;
        if member.health.quarantine.is_some() || member.health.monitoring_error.is_some() {
            continue;
        }
        let quota = member
            .capacity_bytes
            .saturating_sub(member.used_bytes)
            .saturating_sub(member.reserved_bytes);
        if config.local_path.is_none() {
            member.available_bytes = quota;
            available = available.saturating_add(quota);
            continue;
        }
        let observed = match space(config) {
            Ok(observed) => observed,
            Err(_) => {
                unavailable.push(member.id.clone());
                continue;
            }
        };
        member.available_bytes = quota.min(
            observed
                .available
                .saturating_sub(config.min_free_bytes)
                .saturating_sub(local_reserved),
        );
        let budget = filesystems.entry(observed.filesystem).or_insert(Budget {
            available: observed.available,
            reserve: config.min_free_bytes,
            quota: 0,
        });
        // Several roots may share one filesystem: add quotas, never free bytes.
        budget.available = budget.available.min(observed.available);
        budget.reserve = budget.reserve.max(config.min_free_bytes);
        budget.quota = budget.quota.saturating_add(quota);
    }
    for budget in filesystems.into_values() {
        available = available.saturating_add(
            budget.quota.min(
                budget
                    .available
                    .saturating_sub(budget.reserve)
                    .saturating_sub(local_reserved),
            ),
        );
    }
    for member in members {
        let assessment = &mut member.evacuation_capacity;
        assessment.available_bytes = available;
        assessment.sufficient = assessment.required_bytes <= available;
        assessment.unavailable_members = unavailable.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool_object_store::{EvacuationCapacity, MemberHealth};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn member(id: &str, quota: u64, used: u64, reserved: u64) -> (PoolMember, PoolMemberStatus) {
        (
            PoolMember {
                id: id.into(),
                identity: format!("file:///{id}"),
                store: Arc::new(InMemory::new()),
                capacity_bytes: quota,
                local_path: Some(format!("/{id}").into()),
                min_free_bytes: 10,
                health_device: None,
            },
            PoolMemberStatus {
                id: id.into(),
                capacity_bytes: quota,
                used_bytes: used,
                reserved_bytes: reserved,
                objects: 0,
                health: MemberHealth::default(),
                safe_to_remove: false,
                safe_to_pull_with_known_loss: false,
                readable_evacuated: false,
                unreadable_objects: 0,
                pending_uploads: 0,
                cleanup_debts: 0,
                evacuation: "incomplete".into(),
                available_bytes: 0,
                evacuation_capacity: EvacuationCapacity {
                    required_bytes: 100,
                    ..Default::default()
                },
            },
        )
    }

    #[test]
    fn evacuation_capacity_deduplicates_filesystems_and_keeps_reservations() {
        let (a, sa) = member("a", 1000, 0, 20);
        let (b, sb) = member("b", 1000, 0, 30);
        let mut statuses = vec![sa, sb];
        assess_with(&[a, b], &mut statuses, |_| {
            Ok(Space {
                filesystem: 1,
                available: 150,
            })
        });
        assert_eq!(statuses[0].available_bytes, 90);
        assert_eq!(statuses[1].available_bytes, 90);
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 90);
        assert!(!statuses[0].evacuation_capacity.sufficient);
    }

    #[test]
    fn evacuation_capacity_uses_quota_and_healthy_physical_space() {
        let (a, sa) = member("a", 100, 50, 10);
        let (b, mut sb) = member("b", 1000, 0, 0);
        sb.health.quarantine = Some("media failure".into());
        let mut statuses = vec![sa, sb];
        assess_with(&[a, b], &mut statuses, |_| {
            Ok(Space {
                filesystem: 1,
                available: 1000,
            })
        });
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 40);
        assert_eq!(statuses[1].available_bytes, 0);
        assert!(!statuses[1].evacuation_capacity.sufficient);
    }

    #[test]
    fn evacuation_capacity_does_not_invent_space_when_a_root_is_unavailable() {
        let (a, sa) = member("a", 1000, 0, 0);
        let mut statuses = vec![sa];
        assess_with(&[a], &mut statuses, |_| {
            Err(std::io::Error::other("unmounted"))
        });
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 0);
        assert!(!statuses[0].evacuation_capacity.sufficient);
        assert_eq!(statuses[0].evacuation_capacity.unavailable_members, ["a"]);
    }

    #[test]
    fn evacuation_capacity_combines_distinct_devices_and_exact_boundary() {
        let (a, sa) = member("a", 1000, 0, 0);
        let (b, sb) = member("b", 1000, 0, 0);
        let mut statuses = vec![sa, sb];
        assess_with(&[a, b], &mut statuses, |member| {
            Ok(Space {
                filesystem: if member.id == "a" { 1 } else { 2 },
                available: 60,
            })
        });
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 100);
        assert!(statuses[0].evacuation_capacity.sufficient);
    }
}
