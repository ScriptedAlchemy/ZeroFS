//! Conservative evacuation snapshots using the allocator's capacity budgets.
//!
//! Room per member comes from [`super::space`], the same rule placement uses.
use super::space::{self, FilesystemSpace, Full};
use super::{PoolMember, PoolMemberStatus};
use std::collections::BTreeMap;

struct Budget {
    available: u64,
    reserve: u64,
    quota: u64,
}

#[derive(Clone)]
struct Destination {
    available: u64,
    filesystem: Option<u64>,
}

/// `full` holds members whose last write returned no space, with the time it
/// failed; placement skips them, so they offer no room here either.
pub(super) fn assess(
    config: &[PoolMember],
    members: &mut [PoolMemberStatus],
    segments: &BTreeMap<String, Vec<u64>>,
    full: &BTreeMap<String, i64>,
) {
    assess_with(config, members, segments, full, |member| {
        member.validate_binding()?;
        member
            .filesystem_space()
            .expect("assess_with asks only local members")
    });
}

fn assess_with(
    config: &[PoolMember],
    members: &mut [PoolMemberStatus],
    segments: &BTreeMap<String, Vec<u64>>,
    full: &BTreeMap<String, i64>,
    mut space: impl FnMut(&PoolMember) -> std::io::Result<FilesystemSpace>,
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
    let mut destinations = Vec::new();
    for (config, member) in config.iter().zip(members.iter_mut()) {
        member.available_bytes = 0;
        let observed = config.local_path.is_some().then(|| space(config));
        if let Some(Ok(observed)) = &observed {
            member.space.filesystem_free_bytes = Some(observed.available);
            member.space.filesystem_total_bytes = Some(observed.total);
            member.space.declared_exceeds_filesystem = member.capacity_bytes > observed.total;
        }
        let physical = match &observed {
            None => None,
            Some(Ok(observed)) => Some(space::physical_room(
                *observed,
                config.min_free_bytes,
                local_reserved,
            )),
            Some(Err(_)) => None,
        };
        member.space.full = match full.get(&member.id) {
            Some(&since_ms) => Some(Full::WriteReturnedNoSpace { since_ms }),
            None if physical.is_some_and(|room| room.is_none_or(|bytes| bytes == 0)) => {
                Some(Full::BelowFreeSpaceReserve)
            }
            None => None,
        };
        if member.health.quarantine.is_some()
            || member.health.monitoring_error.is_some()
            || full.contains_key(&member.id)
        {
            continue;
        }
        let quota = member
            .capacity_bytes
            .saturating_sub(member.used_bytes)
            .saturating_sub(member.reserved_bytes);
        let observed = match observed {
            None => {
                member.available_bytes = quota;
                available = available.saturating_add(quota);
                destinations.push(Destination {
                    available: quota,
                    filesystem: None,
                });
                continue;
            }
            Some(Ok(observed)) => observed,
            Some(Err(_)) => {
                unavailable.push(member.id.clone());
                continue;
            }
        };
        if let Some(room) = space::room(quota, physical) {
            member.available_bytes = room;
            destinations.push(Destination {
                available: room,
                filesystem: Some(observed.filesystem),
            });
        }
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
    let filesystems: BTreeMap<_, _> = filesystems
        .into_iter()
        .map(|(id, budget)| {
            let physical = budget
                .available
                .saturating_sub(budget.reserve)
                .saturating_sub(local_reserved);
            available = available.saturating_add(budget.quota.min(physical));
            (id, physical)
        })
        .collect();
    for member in members {
        let assessment = &mut member.evacuation_capacity;
        assessment.available_bytes = available;
        assessment.sufficient = assessment.required_bytes <= available
            && segments
                .get(&member.id)
                .is_none_or(|sizes| fits(sizes, destinations.clone(), filesystems.clone()));
        assessment.unavailable_members = unavailable.clone();
    }
}

fn fits(
    sizes: &[u64],
    mut destinations: Vec<Destination>,
    mut filesystems: BTreeMap<u64, u64>,
) -> bool {
    // Match recovery's placement order and first eligible destination. Segments
    // are indivisible, and roots on one filesystem share its remaining bytes.
    sizes.iter().all(|&size| {
        for destination in &mut destinations {
            if destination.available < size {
                continue;
            }
            if let Some(filesystem) = destination.filesystem {
                let remaining = filesystems.get_mut(&filesystem).unwrap();
                if *remaining < size {
                    continue;
                }
                *remaining -= size;
            }
            destination.available -= size;
            return true;
        }
        false
    })
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
                free_space: None,
            },
            PoolMemberStatus {
                id: id.into(),
                capacity_bytes: quota,
                used_bytes: used,
                reserved_bytes: reserved,
                objects: 0,
                health: MemberHealth::default(),
                unavailable_at_startup: false,
                safe_to_remove: false,
                safe_to_pull_with_known_loss: false,
                readable_evacuated: false,
                unreadable_objects: 0,
                pending_uploads: 0,
                cleanup_debts: 0,
                evacuation: "incomplete".into(),
                available_bytes: 0,
                space: Default::default(),
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
        assess_with(
            &[a, b],
            &mut statuses,
            &BTreeMap::new(),
            &BTreeMap::new(),
            |_| {
                Ok(FilesystemSpace {
                    filesystem: 1,
                    available: 150,
                    total: 1000,
                })
            },
        );
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
        assess_with(
            &[a, b],
            &mut statuses,
            &BTreeMap::new(),
            &BTreeMap::new(),
            |_| {
                Ok(FilesystemSpace {
                    filesystem: 1,
                    available: 1000,
                    total: 1000,
                })
            },
        );
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 40);
        assert_eq!(statuses[1].available_bytes, 0);
        assert!(!statuses[1].evacuation_capacity.sufficient);
    }

    #[test]
    fn evacuation_capacity_does_not_invent_space_when_a_root_is_unavailable() {
        let (a, sa) = member("a", 1000, 0, 0);
        let mut statuses = vec![sa];
        assess_with(
            &[a],
            &mut statuses,
            &BTreeMap::new(),
            &BTreeMap::new(),
            |_| Err(std::io::Error::other("unmounted")),
        );
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 0);
        assert!(!statuses[0].evacuation_capacity.sufficient);
        assert_eq!(statuses[0].evacuation_capacity.unavailable_members, ["a"]);
    }

    #[test]
    fn evacuation_capacity_combines_distinct_devices_and_exact_boundary() {
        let (a, sa) = member("a", 1000, 0, 0);
        let (b, sb) = member("b", 1000, 0, 0);
        let mut statuses = vec![sa, sb];
        assess_with(
            &[a, b],
            &mut statuses,
            &BTreeMap::new(),
            &BTreeMap::new(),
            |member| {
                Ok(FilesystemSpace {
                    filesystem: if member.id == "a" { 1 } else { 2 },
                    available: 60,
                    total: 1000,
                })
            },
        );
        assert_eq!(statuses[0].evacuation_capacity.available_bytes, 100);
        assert!(statuses[0].evacuation_capacity.sufficient);
    }

    #[test]
    fn whole_segments_share_the_filesystem_budget_across_destination_roots() {
        for (shared_free, sufficient) in [(18, false), (22, true)] {
            let (source, mut source_status) = member("source", 12, 12, 0);
            source_status.health.quarantine = Some("media failure".into());
            source_status.evacuation_capacity.required_bytes = 12;
            let (a, sa) = member("a", 8, 0, 0);
            let (b, sb) = member("b", 8, 0, 0);
            let (c, sc) = member("c", 4, 0, 0);
            let mut statuses = vec![source_status, sa, sb, sc];
            let segments = BTreeMap::from([("source".into(), vec![6, 6])]);
            assess_with(
                &[source, a, b, c],
                &mut statuses,
                &segments,
                &BTreeMap::new(),
                |member| {
                    Ok(FilesystemSpace {
                        filesystem: if member.id == "c" { 2 } else { 1 },
                        available: if member.id == "c" { 14 } else { shared_free },
                        total: 1000,
                    })
                },
            );
            assert!(statuses[0].evacuation_capacity.available_bytes >= 12);
            assert_eq!(statuses[0].evacuation_capacity.sufficient, sufficient);
        }
    }

    #[test]
    fn zero_byte_segments_still_require_a_destination_above_its_free_space_reserve() {
        for (available, sufficient) in [(9, false), (10, true)] {
            let (source, mut source_status) = member("source", 1, 0, 0);
            source_status.health.quarantine = Some("media failure".into());
            source_status.evacuation_capacity.required_bytes = 0;
            let (target, target_status) = member("target", 1, 0, 0);
            let mut statuses = vec![source_status, target_status];
            let segments = BTreeMap::from([("source".into(), vec![0])]);
            assess_with(
                &[source, target],
                &mut statuses,
                &segments,
                &BTreeMap::new(),
                |_| {
                    Ok(FilesystemSpace {
                        filesystem: 1,
                        available,
                        total: 1000,
                    })
                },
            );
            assert_eq!(statuses[0].evacuation_capacity.sufficient, sufficient);
        }
    }
}
