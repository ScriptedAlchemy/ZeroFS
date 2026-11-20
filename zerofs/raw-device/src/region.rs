//! Byte bounds for one engine-owned region within a single device claim.
use crate::{BLOCK_SIZE, Error, Result};
use std::{fs::File, io, os::unix::fs::FileExt};

#[derive(Debug)]
pub(crate) struct Region {
    file: File,
    start: u64,
    pub(crate) len: u64,
}

impl Region {
    pub(crate) fn whole(file: File, len: u64) -> Result<Self> {
        crate::format::check_device_size(len).map_err(Error::InvalidGeometry)?;
        if device_size(&file)? != len {
            return Err(Error::InvalidGeometry(
                "device must end exactly at the supplied size".into(),
            ));
        }
        Self::new(file, len, 0, len)
    }

    pub(crate) fn new(file: File, device_len: u64, start: u64, len: u64) -> Result<Self> {
        crate::format::check_device_size(device_len).map_err(Error::InvalidGeometry)?;
        if !start.is_multiple_of(BLOCK_SIZE)
            || !len.is_multiple_of(BLOCK_SIZE)
            || len == 0
            || start.checked_add(len).is_none_or(|end| end > device_len)
        {
            return Err(Error::InvalidGeometry(
                "unaligned or out-of-device region".into(),
            ));
        }
        Ok(Self { file, start, len })
    }

    fn absolute(&self, relative: u64, bytes: usize) -> io::Result<u64> {
        if relative
            .checked_add(bytes as u64)
            .is_none_or(|end| end > self.len)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O outside raw region",
            ));
        }
        Ok(self.start + relative)
    }

    pub(crate) fn read_exact_at(&self, output: &mut [u8], offset: u64) -> io::Result<()> {
        self.file
            .read_exact_at(output, self.absolute(offset, output.len())?)
    }

    pub(crate) fn write_all_at(&self, input: &[u8], offset: u64) -> io::Result<()> {
        self.file
            .write_all_at(input, self.absolute(offset, input.len())?)
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

fn device_size(file: &File) -> Result<u64> {
    let metadata = file.metadata()?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::{fd::AsRawFd, unix::fs::FileTypeExt};

        if metadata.file_type().is_block_device() {
            let mut size = 0u64;
            // BLKGETSIZE64 returns the size of this descriptor's block device.
            let result = unsafe { libc::ioctl(file.as_raw_fd(), 0x80081272, &mut size) };
            if result < 0 {
                return Err(io::Error::last_os_error().into());
            }
            return Ok(size);
        }
    }
    Err(Error::InvalidGeometry(
        "raw descriptor must be a regular file or a Linux block device".into(),
    ))
}
