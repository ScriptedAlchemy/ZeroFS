//! Room on a pool member: the declared capacity an operator typed, bounded by
//! the free space its backing filesystem really reports.
//!
//! Every placement and every status report reads the real figure through
//! [`PoolMember::filesystem_space`] and combines it with the declared figure in
//! [`room`]. Nothing else calls `statvfs`.
//!
//! Each reading is taken when it is used: a reservation reads it inside the
//! catalog write transaction that records the reservation, and status reads it
//! once per request. Nothing caches it, so it is never older than one catalog
//! transaction. It can still be wrong by the time the bytes land, because
//! something outside the pool may fill the filesystem between the check and the
//! write, and some filesystems (ZFS, for one) refuse writes before `f_bavail`
//! reaches zero. That race is handled after the fact: a member write that
//! returns ENOSPC marks the member full and the write moves to another member.
use super::PoolMember;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

/// One reading of a member's backing filesystem.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilesystemSpace {
    /// Device id, so roots that share one filesystem share its free bytes.
    pub filesystem: u64,
    /// Bytes an unprivileged writer may still use (`f_bavail * f_frsize`).
    pub available: u64,
    /// Size of the filesystem (`f_blocks * f_frsize`).
    pub total: u64,
}

/// Where a local member's real free space comes from. Production uses
/// [`Statvfs`]; tests inject exact numbers.
pub trait FreeSpace: Send + Sync + fmt::Debug {
    fn observe(&self) -> std::io::Result<FilesystemSpace>;
}

#[derive(Debug)]
pub struct Statvfs(pub PathBuf);

impl FreeSpace for Statvfs {
    fn observe(&self) -> std::io::Result<FilesystemSpace> {
        Ok(FilesystemSpace {
            filesystem: std::fs::metadata(&self.0)?.dev(),
            available: fs4::available_space(&self.0)?,
            total: fs4::total_space(&self.0)?,
        })
    }
}

impl PoolMember {
    /// A local member's real free space; `None` for a remote member, which has
    /// no filesystem to ask and is bounded only by its declared capacity and
    /// by its real write errors.
    pub(super) fn filesystem_space(&self) -> Option<std::io::Result<FilesystemSpace>> {
        let path = self.local_path.as_ref()?;
        Some(match &self.free_space {
            Some(source) => source.observe(),
            None => Statvfs(path.clone()).observe(),
        })
    }

    /// Replaces the member's `statvfs` reading with an injected source.
    #[cfg(test)]
    pub fn with_free_space(mut self, source: std::sync::Arc<dyn FreeSpace>) -> Self {
        self.free_space = Some(source);
        self
    }
}

/// Bytes the filesystem can still take for the pool after the member's
/// `reserve_free_gb` and every outstanding local reservation (which may not
/// have reached the device yet). `None` means the filesystem is already at or
/// below that floor: the member is full, and not even a zero-byte object fits.
pub(super) fn physical_room(
    space: FilesystemSpace,
    reserve: u64,
    local_reserved: u64,
) -> Option<u64> {
    space
        .available
        .checked_sub(reserve)?
        .checked_sub(local_reserved)
}

/// The one allocatable rule: the declared capacity left, capped by the real
/// room when the member has a filesystem. `None` means no object fits at all.
pub(super) fn room(declared_remaining: u64, physical: Option<Option<u64>>) -> Option<u64> {
    match physical {
        None => Some(declared_remaining),
        Some(physical) => physical.map(|bytes| bytes.min(declared_remaining)),
    }
}

/// How long a member that reported no space stays out of placement before
/// writes may try it again. A remote store has no free-space figure to watch;
/// a local quota or inode limit can clear without changing free bytes either.
pub(super) const FULL_RECHECK: Duration = Duration::from_secs(30);

/// Why placement currently skips a member for lack of room. Full is not a
/// media fault: the member stays readable, healthy and unquarantined.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum Full {
    /// A write to the member returned ENOSPC or EDQUOT.
    WriteReturnedNoSpace {
        /// Server wall-clock milliseconds of the failed write.
        since_ms: i64,
    },
    /// The filesystem's free bytes are at or below `reserve_free_gb` plus the
    /// pool's outstanding local reservations.
    BelowFreeSpaceReserve,
}

#[derive(Clone, Debug)]
struct NoSpaceMark {
    since_ms: i64,
    at: Instant,
    free_at_failure: Option<u64>,
}

/// Members whose last write returned no-space. Process memory only: after a
/// restart the first write to such a member simply fails over again.
#[derive(Debug, Default)]
pub(super) struct NoSpaceMarks(StdMutex<HashMap<String, NoSpaceMark>>);

impl NoSpaceMarks {
    pub(super) fn mark(&self, member: &PoolMember) {
        let free_at_failure = match member.filesystem_space() {
            Some(Ok(space)) => Some(space.available),
            _ => None,
        };
        tracing::warn!(
            member = %member.id,
            free_bytes = ?free_at_failure,
            "pool member is full: a write returned no space; placing new data on other members until free space grows or the retry interval expires"
        );
        self.0.lock().unwrap().insert(
            member.id.clone(),
            NoSpaceMark {
                since_ms: chrono::Utc::now().timestamp_millis(),
                at: Instant::now(),
                free_at_failure,
            },
        );
    }

    /// Members that are still full, keyed by id with the time they filled.
    /// A local member leaves the set once its filesystem reports more free
    /// bytes than it had when the write failed, or after [`FULL_RECHECK`].
    /// A remote member leaves after the same interval. Placement still checks
    /// the current quota and free-space reserve before trying another write.
    pub(super) fn current(&self, members: &[PoolMember]) -> BTreeMap<String, i64> {
        let mut marks = self.0.lock().unwrap();
        marks.retain(|id, mark| {
            let Some(member) = members.iter().find(|member| &member.id == id) else {
                return false;
            };
            let recovered = match (member.filesystem_space(), mark.free_at_failure) {
                (Some(Ok(space)), Some(before)) => {
                    space.available > before || mark.at.elapsed() >= FULL_RECHECK
                }
                (Some(Ok(_)), None) => true,
                (Some(Err(_)), _) => false,
                (None, _) => mark.at.elapsed() >= FULL_RECHECK,
            };
            if recovered {
                tracing::info!(member = %id, "pool member free space grew or its retry interval elapsed; placement may try it again");
            }
            !recovered
        });
        marks
            .iter()
            .map(|(id, mark)| (id.clone(), mark.since_ms))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FixedSpace(u64);

    impl FreeSpace for FixedSpace {
        fn observe(&self) -> std::io::Result<FilesystemSpace> {
            Ok(space(self.0))
        }
    }

    fn member(available: Option<u64>) -> PoolMember {
        PoolMember {
            id: "member".into(),
            identity: "test-member".into(),
            store: std::sync::Arc::new(object_store::memory::InMemory::new()),
            capacity_bytes: 1000,
            local_path: available.map(|_| "/member".into()),
            min_free_bytes: 10,
            health_device: None,
            free_space: available.map(|available| std::sync::Arc::new(FixedSpace(available)) as _),
        }
    }

    #[test]
    fn a_local_no_space_mark_expires_without_a_free_byte_increase() {
        // EDQUOT and inode exhaustion can clear without increasing f_bavail.
        // That byte counter cannot be the only way to retry the member.
        let member = member(Some(100));
        let marks = NoSpaceMarks::default();
        marks.mark(&member);
        assert!(
            marks
                .current(std::slice::from_ref(&member))
                .contains_key(&member.id)
        );

        marks.0.lock().unwrap().get_mut(&member.id).unwrap().at =
            Instant::now() - Duration::from_secs(31);
        assert!(
            marks.current(std::slice::from_ref(&member)).is_empty(),
            "a recovered quota must not require a process restart"
        );
    }

    #[test]
    fn local_free_space_growth_allows_a_retry_before_the_recheck_interval() {
        let mut member = member(Some(100));
        let marks = NoSpaceMarks::default();
        marks.mark(&member);
        member.free_space = Some(std::sync::Arc::new(FixedSpace(101)));
        assert!(marks.current(std::slice::from_ref(&member)).is_empty());
    }

    #[test]
    fn remote_no_space_marks_also_wait_before_retrying() {
        let member = member(None);
        let marks = NoSpaceMarks::default();
        marks.mark(&member);
        assert!(
            marks
                .current(std::slice::from_ref(&member))
                .contains_key(&member.id)
        );
        marks.0.lock().unwrap().get_mut(&member.id).unwrap().at =
            Instant::now() - Duration::from_secs(31);
        assert!(marks.current(std::slice::from_ref(&member)).is_empty());
    }

    fn space(available: u64) -> FilesystemSpace {
        FilesystemSpace {
            filesystem: 1,
            available,
            total: 1000,
        }
    }

    #[test]
    fn declared_capacity_is_capped_by_real_free_space_after_the_reserve() {
        // 6,001 GB declared on a filesystem with 100 bytes free and a 10-byte
        // reserve leaves 90 bytes, not terabytes.
        let declared = 6_001_000_000_000;
        assert_eq!(
            room(declared, Some(physical_room(space(100), 10, 0))),
            Some(90)
        );
        assert_eq!(room(50, Some(physical_room(space(100), 10, 0))), Some(50));
        assert_eq!(room(50, Some(physical_room(space(100), 10, 30))), Some(50));
        assert_eq!(room(80, Some(physical_room(space(100), 10, 30))), Some(60));
    }

    #[test]
    fn a_filesystem_at_or_below_its_reserve_has_no_room_for_any_object() {
        assert_eq!(physical_room(space(9), 10, 0), None);
        assert_eq!(physical_room(space(10), 10, 0), Some(0));
        assert_eq!(physical_room(space(30), 10, 25), None);
        assert_eq!(room(100, Some(None)), None);
    }

    #[test]
    fn a_remote_member_is_bounded_only_by_its_declared_capacity() {
        assert_eq!(room(100, None), Some(100));
        assert_eq!(room(0, None), Some(0));
    }
}
