//! Verify that every logical block of the disposable backing file has its
//! own physical allocation. st_blocks alone cannot rule out interior holes.
use super::require;
use std::{fs::File, io, os::fd::AsRawFd};

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Extent {
    logical: u64,
    physical: u64,
    length: u64,
    reserved64: [u64; 2],
    flags: u32,
    reserved: [u32; 3],
}

#[repr(C)]
struct Fiemap {
    start: u64,
    length: u64,
    flags: u32,
    mapped: u32,
    count: u32,
    reserved: u32,
    extents: [Extent; 64],
}

pub(super) fn require_allocated(file: &File, size: u64) -> io::Result<()> {
    let mut next = 0;
    while next < size {
        let mut map = Fiemap {
            start: next,
            length: size - next,
            flags: 1,
            mapped: 0,
            count: 64,
            reserved: 0,
            extents: [Extent::default(); 64],
        };
        // FS_IOC_FIEMAP encodes the size of its 32-byte fixed header.
        super::checked(unsafe { libc::ioctl(file.as_raw_fd(), 0xc020660b, &mut map) })?;
        require(
            map.mapped > 0 && map.mapped <= map.count,
            "fixture has an unallocated range",
        )?;
        for extent in &map.extents[..map.mapped as usize] {
            // FIEMAP_EXTENT_LAST and FIEMAP_EXTENT_UNWRITTEN are valid on a
            // newly fallocated ext4 inode. Reject unknown/shared/delayed data.
            require(
                extent.logical == next
                    && extent.physical != 0
                    && extent.length > 0
                    && extent.flags & !(1 | 0x800) == 0,
                "fixture extent is sparse, shared, or lacks an exclusive allocation",
            )?;
            next = next
                .checked_add(extent.length)
                .filter(|n| *n <= size)
                .ok_or_else(|| io::Error::other("invalid fixture extent boundary"))?;
        }
    }
    Ok(())
}
