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
//! reaches zero.
use super::PoolMember;
use std::fmt;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

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

#[cfg(test)]
mod tests {
    use super::*;

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
