use crate::fs::errors::FsError;

#[derive(Debug, Clone, Copy)]
pub enum P9Error {
    BadFid,
    FidNotOpen,
    FidAlreadyOpen,
    FidInUse,
    InvalidEncoding,
    InvalidArgument,
    Overflow,
    NotADirectory,
    NotASymlink,
    InvalidDeviceType,
    LockConflict,
    NotSupported,
    NotImplemented,
    Fs(FsError),
}

pub type P9Result<T> = Result<T, P9Error>;

impl P9Error {
    pub fn to_errno(self) -> u32 {
        match self {
            P9Error::BadFid | P9Error::FidNotOpen => crate::linux_errno::EBADF,
            P9Error::FidAlreadyOpen => crate::linux_errno::EBUSY,
            P9Error::FidInUse
            | P9Error::InvalidEncoding
            | P9Error::InvalidArgument
            | P9Error::NotASymlink
            | P9Error::InvalidDeviceType => crate::linux_errno::EINVAL,
            P9Error::Overflow => crate::linux_errno::EOVERFLOW,
            P9Error::NotADirectory => crate::linux_errno::ENOTDIR,
            P9Error::LockConflict => crate::linux_errno::EAGAIN,
            P9Error::NotSupported => crate::linux_errno::EOPNOTSUPP,
            P9Error::NotImplemented => crate::linux_errno::ENOSYS,
            P9Error::Fs(e) => e.to_errno(),
        }
    }
}

impl From<FsError> for P9Error {
    fn from(e: FsError) -> Self {
        P9Error::Fs(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_errnos_use_the_linux_wire_abi() {
        for (error, expected) in [
            (P9Error::BadFid, 9),
            (P9Error::FidNotOpen, 9),
            (P9Error::FidAlreadyOpen, 16),
            (P9Error::FidInUse, 22),
            (P9Error::InvalidEncoding, 22),
            (P9Error::InvalidArgument, 22),
            (P9Error::Overflow, 75),
            (P9Error::NotADirectory, 20),
            (P9Error::NotASymlink, 22),
            (P9Error::InvalidDeviceType, 22),
            (P9Error::LockConflict, 11),
            (P9Error::NotSupported, 95),
            (P9Error::NotImplemented, 38),
        ] {
            assert_eq!(error.to_errno(), expected, "{error:?}");
        }
    }
}
