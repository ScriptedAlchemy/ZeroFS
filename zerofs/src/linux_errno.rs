//! Linux errno values carried by ZeroFS wire protocols.
//!
//! These are protocol ABI values, not constants from the compilation host.

pub(crate) const EPERM: u32 = 1;
pub(crate) const ENOENT: u32 = 2;
pub(crate) const EIO: u32 = 5;
pub(crate) const EBADF: u32 = 9;
pub(crate) const EAGAIN: u32 = 11;
pub(crate) const EACCES: u32 = 13;
pub(crate) const EBUSY: u32 = 16;
pub(crate) const EEXIST: u32 = 17;
pub(crate) const ENOTDIR: u32 = 20;
pub(crate) const EISDIR: u32 = 21;
pub(crate) const EINVAL: u32 = 22;
pub(crate) const ENOSPC: u32 = 28;
pub(crate) const EROFS: u32 = 30;
pub(crate) const EMLINK: u32 = 31;
pub(crate) const ENAMETOOLONG: u32 = 36;
pub(crate) const ENOSYS: u32 = 38;
pub(crate) const ENOTEMPTY: u32 = 39;
pub(crate) const EOVERFLOW: u32 = 75;
pub(crate) const EOPNOTSUPP: u32 = 95;
pub(crate) const ESTALE: u32 = 116;
