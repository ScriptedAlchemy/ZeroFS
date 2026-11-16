use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Field<T> {
    Observed(T),
    Unavailable { reason: Reason, errno: Option<i32> },
}

impl<T: Serialize> Serialize for Field<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        match self {
            Self::Observed(value) => {
                let mut object = serializer.serialize_struct("Field", 2)?;
                object.serialize_field("status", "observed")?;
                object.serialize_field("value", value)?;
                object.end()
            }
            Self::Unavailable { reason, errno } => {
                let mut object = serializer.serialize_struct("Field", 3)?;
                object.serialize_field("status", "unavailable")?;
                object.serialize_field("reason", reason)?;
                object.serialize_field("errno", errno)?;
                object.end()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Reason {
    PermissionDenied,
    NotSupportedByFilesystem,
    FormatNotSupportedByFilesystem,
    NotImplementedOnPlatform,
    #[cfg(target_os = "linux")]
    NotApplicableOnPlatform,
    NotCollectedForKind,
    NotRequested,
    NotDecoded,
    EntryChanged,
    MountBoundary,
    IncompleteTraversal,
    Io,
}

impl Reason {
    pub(super) fn counts_as_complete(self) -> bool {
        match self {
            Self::NotCollectedForKind
            | Self::NotRequested
            | Self::NotDecoded
            | Self::FormatNotSupportedByFilesystem => true,
            #[cfg(target_os = "linux")]
            Self::NotApplicableOnPlatform => true,
            _ => false,
        }
    }
}

impl<T> Field<T> {
    pub(super) fn is_complete(&self) -> bool {
        match self {
            Self::Observed(_) => true,
            Self::Unavailable { reason, .. } => reason.counts_as_complete(),
        }
    }

    pub(super) fn format_probe(self) -> Self {
        match self {
            Self::Unavailable {
                reason: Reason::NotSupportedByFilesystem,
                errno,
            } => Self::Unavailable {
                reason: Reason::FormatNotSupportedByFilesystem,
                errno,
            },
            other => other,
        }
    }
    pub(super) fn unavailable(reason: Reason) -> Self {
        Self::Unavailable {
            reason,
            errno: None,
        }
    }

    pub(super) fn error(error: rustix::io::Errno) -> Self {
        let reason = match error {
            rustix::io::Errno::ACCESS | rustix::io::Errno::PERM => Reason::PermissionDenied,
            rustix::io::Errno::OPNOTSUPP => Reason::NotSupportedByFilesystem,
            rustix::io::Errno::NOSYS => Reason::NotImplementedOnPlatform,
            _ => Reason::Io,
        };
        Self::Unavailable {
            reason,
            errno: Some(error.raw_os_error()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RawBytes {
    pub hex: String,
    pub display: String,
}

impl RawBytes {
    pub(super) fn new(bytes: &[u8]) -> Self {
        Self {
            hex: hex(bytes),
            display: String::from_utf8_lossy(bytes).into_owned(),
        }
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EntryKind {
    File,
    Directory,
    Symlink,
    Fifo,
    Socket,
    CharacterDevice,
    BlockDevice,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Identity {
    pub dev: u64,
    pub ino: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Owner {
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Timestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Times {
    pub access: Timestamp,
    pub modification: Timestamp,
    pub change: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Extent {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Xattr {
    pub name: RawBytes,
    pub value: Field<RawBytes>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RawAcl {
    pub format: &'static str,
    pub raw: Field<Option<RawBytes>>,
    pub decoded: Field<()>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Entry {
    pub path: RawBytes,
    pub kind: Field<EntryKind>,
    pub identity: Field<Identity>,
    pub nlink: Field<u64>,
    pub owner: Field<Owner>,
    pub mode: Field<u32>,
    pub times: Field<Times>,
    pub birth_time: Field<Timestamp>,
    pub size: Field<u64>,
    pub allocated_bytes: Field<u64>,
    pub data_extents: Field<Vec<Extent>>,
    pub symlink_target: Field<RawBytes>,
    pub rdev: Field<u64>,
    pub platform_flags: Field<u32>,
    pub xattrs: Field<Vec<Xattr>>,
    pub acl: Field<Vec<RawAcl>>,
    pub content_sha256: Field<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Filesystem {
    pub type_name: Option<String>,
    pub type_magic: u64,
    pub network: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct Header {
    pub schema: u32,
    pub tool_version: &'static str,
    pub collector: &'static str,
    pub root: RawBytes,
    pub root_identity: Field<Identity>,
    pub filesystem: Field<Filesystem>,
    pub hashing: &'static str,
    pub collision_rules: [&'static str; 2],
    pub atime_notice: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct HardlinkGroup {
    pub dev: u64,
    pub ino: u64,
    pub nlink: Field<u64>,
    pub paths_in_tree: Vec<RawBytes>,
    pub links_outside_tree: Field<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct NameCollision {
    pub dir: RawBytes,
    pub rule: &'static str,
    pub names: Vec<RawBytes>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub entries: u64,
    pub non_utf8_names: u64,
    pub unavailable_by_reason: std::collections::BTreeMap<String, u64>,
    pub complete: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(crate) enum Record {
    Header(Header),
    Entry(Box<Entry>),
    HardlinkGroup(HardlinkGroup),
    NameCollision(NameCollision),
    DirectoryUnreadable {
        path: RawBytes,
        error: Field<()>,
    },
    EntryChanged {
        path: RawBytes,
        error: Field<()>,
    },
    MountBoundary {
        path: RawBytes,
        identity: Field<Identity>,
        error: Field<()>,
    },
    Summary(Summary),
    Digest {
        algorithm: &'static str,
        sha256: String,
    },
}
