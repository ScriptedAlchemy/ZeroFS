//! Durable object store over a raw block device that the caller has already
//! claimed exclusively.
//!
//! The crate does not open, claim, or discover devices. The caller passes in an
//! open [`std::fs::File`] that it holds exclusively (for example a block device
//! opened with `O_EXCL`) along with the exact device size it measured. The
//! store takes ownership of the file and uses positioned reads and writes plus
//! `fdatasync` on it.
//!
//! # Lifecycle
//!
//! * [`RawStore::provision`] reads the whole device and refuses it unless every
//!   byte is zero, so a partition table, filesystem signature, or data
//!   anywhere on the device stops it. There is no override. A device that
//!   already carries a ZeroFS label is reported as
//!   [`Error::AlreadyProvisioned`]. Otherwise it writes a label that binds the
//!   device to one pool UUID, member UUID, and [`Role`].
//! * [`RawStore::open`] reopens a labelled device. The supplied
//!   [`Identity`] and device size must match the label exactly.
//! * [`RawStore::put`] and [`RawStore::delete`] append to the log. GET sees
//!   them immediately, but they are **not durable** until [`RawStore::flush`]
//!   returns `Ok`.
//! * [`RawStore::get`] verifies the SHA-256 of every payload it returns.
//!
//! # Crash semantics
//!
//! A flush writes the new checkpoint label to both label slots, with a
//! barrier after each, before it returns. Consequences:
//!
//! * Reopening after a crash or power loss yields the state of the last
//!   acknowledged flush, or of a flush that was in progress when the crash
//!   hit. It never yields anything older than the last acknowledged flush.
//! * Damage to either label slot alone after a flush is acknowledged loses
//!   nothing: the other slot still holds the same checkpoint.
//!   [`RawStore::label_degraded`] reports it, and the next `flush` rewrites
//!   both slots.
//! * If both slots are damaged, `open` returns [`Error::Corrupt`] rather than
//!   guessing.
//! * Records appended after the last durable checkpoint are discarded on
//!   reopen, whether complete or torn. Damage inside the durable range is
//!   never truncated silently: it surfaces as [`Error::Corrupt`].
//!
//! See `format.rs` for the on-disk layout.

#![cfg_attr(not(unix), allow(unused))]

#[cfg(target_os = "linux")]
pub mod claim;
mod format;
pub mod journal_region;
pub mod landing;
pub mod p3;
mod region;
mod store;

use std::fmt;
use std::io;

pub use store::{ObjectHandle, ObjectInfo, RawReader, RawStore};

/// Unit of every on-disk structure and of payload alignment. The device size
/// must be an exact multiple of it.
pub const BLOCK_SIZE: u64 = 4096;
/// Smallest device the store accepts: the label plus room for a few records.
pub const MIN_DEVICE_SIZE: u64 = 1 << 20;
/// Largest device the store accepts (1 PiB). Larger sizes are refused rather
/// than trusted.
pub const MAX_DEVICE_SIZE: u64 = 1 << 50;
/// Longest key, in bytes. Keys must be nonempty.
pub const MAX_KEY_LEN: usize = 1024;
/// Largest single object, in bytes. Free space bounds it further.
pub const MAX_OBJECT_LEN: u64 = 1 << 30;

/// What a member device holds within its pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Role {
    Data = 1,
    Metadata = 2,
    Journal = 3,
    Catalog = 4,
    KeyEnvelope = 5,
    Scratch = 6,
    Cache = 7,
}

impl Role {
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Data),
            2 => Some(Self::Metadata),
            3 => Some(Self::Journal),
            4 => Some(Self::Catalog),
            5 => Some(Self::KeyEnvelope),
            6 => Some(Self::Scratch),
            7 => Some(Self::Cache),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Metadata => "metadata",
            Self::Journal => "journal",
            Self::Catalog => "catalog",
            Self::KeyEnvelope => "key-envelope",
            Self::Scratch => "scratch",
            Self::Cache => "cache",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The binding a label records and every open must present. Neither UUID may
/// be nil (all zero).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Identity {
    pub pool_uuid: [u8; 16],
    pub member_uuid: [u8; 16],
    pub role: Role,
}

impl Identity {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.pool_uuid == [0; 16] {
            return Err(Error::InvalidArgument("pool UUID is nil"));
        }
        if self.member_uuid == [0; 16] {
            return Err(Error::InvalidArgument("member UUID is nil"));
        }
        Ok(())
    }
}

/// Format a UUID in the canonical 8-4-4-4-12 hex form.
pub fn format_uuid(uuid: &[u8; 16]) -> String {
    let hex: String = uuid.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pool {} member {} role {}",
            format_uuid(&self.pool_uuid),
            format_uuid(&self.member_uuid),
            self.role
        )
    }
}

/// Where the log stands. Offsets are absolute byte offsets on the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    /// Device size, exactly as supplied and recorded.
    pub device_size: u64,
    /// First byte of the log region.
    pub log_start: u64,
    /// End of everything appended, durable or not.
    pub tail: u64,
    /// End of the log as of the last durable checkpoint.
    pub durable_tail: u64,
    /// Bytes still available for appends.
    pub free: u64,
    /// Generation of the last durable checkpoint.
    pub generation: u64,
}

#[derive(Debug)]
pub enum Error {
    /// The device returned an I/O error.
    Io(io::Error),
    /// The append does not fit. Nothing was written.
    NoSpace { needed: u64, available: u64 },
    /// The label binds the device to a different pool, member, or role.
    IdentityMismatch { expected: Identity, found: Identity },
    /// The label records a different device size than the caller supplied.
    DeviceSizeMismatch { recorded: u64, supplied: u64 },
    /// The supplied size is out of bounds, not block aligned, or not the
    /// device's actual size.
    InvalidGeometry(String),
    /// No ZeroFS label is present.
    NotProvisioned,
    /// `provision` found a valid ZeroFS label.
    AlreadyProvisioned { found: Identity },
    /// `provision` found a nonzero byte; nothing was written.
    NotBlank { offset: u64 },
    /// The label is from an unsupported format version.
    UnsupportedVersion(u32),
    /// On-device data inside the durable range failed validation.
    Corrupt { offset: u64, reason: String },
    /// A key, value, or identity is outside the accepted bounds.
    InvalidArgument(&'static str),
    /// An earlier write or sync failed, so the device state is unknown. Reopen
    /// the device to recover the last durable state.
    Poisoned,
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "device I/O error: {err}"),
            Self::NoSpace { needed, available } => {
                write!(f, "no space: need {needed} bytes, {available} available")
            }
            Self::IdentityMismatch { expected, found } => {
                let mut fields = Vec::new();
                if expected.pool_uuid != found.pool_uuid {
                    fields.push("pool");
                }
                if expected.member_uuid != found.member_uuid {
                    fields.push("member");
                }
                if expected.role != found.role {
                    fields.push("role");
                }
                write!(
                    f,
                    "device label binding mismatch ({}): expected {expected}, label has {found}",
                    fields.join(", ")
                )
            }
            Self::DeviceSizeMismatch { recorded, supplied } => write!(
                f,
                "device size mismatch: label records {recorded} bytes, caller supplied {supplied}"
            ),
            Self::InvalidGeometry(reason) => write!(f, "invalid geometry: {reason}"),
            Self::NotProvisioned => write!(f, "device carries no ZeroFS label"),
            Self::AlreadyProvisioned { found } => {
                write!(f, "device already carries a ZeroFS label for {found}")
            }
            Self::NotBlank { offset } => write!(
                f,
                "device is not blank: nonzero byte at offset {offset}; refusing to provision"
            ),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported label version {version}")
            }
            Self::Corrupt { offset, reason } => {
                write!(f, "corrupt data at byte {offset}: {reason}")
            }
            Self::InvalidArgument(reason) => write!(f, "invalid argument: {reason}"),
            Self::Poisoned => write!(f, "store poisoned by an earlier write failure; reopen it"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Converts to an `io::Error` whose message is this error's `Display` text, so
/// binding and geometry details survive an `io::Result` boundary.
impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        let kind = match &err {
            Error::Io(inner) => return io::Error::new(inner.kind(), err.to_string()),
            Error::NoSpace { .. } => io::ErrorKind::StorageFull,
            Error::IdentityMismatch { .. }
            | Error::DeviceSizeMismatch { .. }
            | Error::NotProvisioned
            | Error::UnsupportedVersion(_)
            | Error::Corrupt { .. } => io::ErrorKind::InvalidData,
            Error::AlreadyProvisioned { .. } | Error::NotBlank { .. } => {
                io::ErrorKind::AlreadyExists
            }
            Error::InvalidGeometry(_) | Error::InvalidArgument(_) => io::ErrorKind::InvalidInput,
            Error::Poisoned => io::ErrorKind::Other,
        };
        io::Error::new(kind, err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_mismatch_names_the_differing_fields_and_both_bindings() {
        let expected = Identity {
            pool_uuid: [0x11; 16],
            member_uuid: [0x22; 16],
            role: Role::Data,
        };
        let found = Identity {
            role: Role::Journal,
            member_uuid: [0x33; 16],
            ..expected
        };
        let err: io::Error = Error::IdentityMismatch { expected, found }.into();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "device label binding mismatch (member, role): expected pool \
             11111111-1111-1111-1111-111111111111 member 22222222-2222-2222-2222-222222222222 \
             role data, label has pool 11111111-1111-1111-1111-111111111111 member \
             33333333-3333-3333-3333-333333333333 role journal"
        );
    }

    #[test]
    fn io_conversion_keeps_kind_and_detail() {
        let err: io::Error = Error::NoSpace {
            needed: 8192,
            available: 4096,
        }
        .into();
        assert_eq!(err.kind(), io::ErrorKind::StorageFull);
        assert!(err.to_string().contains("8192"));
        let err: io::Error =
            Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "EACCES")).into();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("EACCES"));
    }
}
