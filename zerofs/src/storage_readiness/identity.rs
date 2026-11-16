use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum FilesystemIdentity {
    Uuid {
        uuid: String,
    },
    // Strings preserve the full unsigned GUID range through TOML's signed
    // 64-bit integer boundary.
    Zfs {
        pool_guid: String,
        dataset_guid: String,
    },
}

impl FilesystemIdentity {
    pub(super) fn capacity_domain(&self) -> String {
        match self {
            Self::Uuid { uuid } => format!("uuid:{uuid}"),
            Self::Zfs { pool_guid, .. } => format!("zfs-pool:{pool_guid}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MountedFilesystem {
    pub(super) mountpoint: PathBuf,
    pub(super) filesystem_root: PathBuf,
    pub(super) identity: FilesystemIdentity,
    pub(super) writable: bool,
    pub(super) unlocked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub(crate) struct Serial(String);

impl TryFrom<String> for Serial {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.trim().is_empty() || value.trim() != value {
            return Err("device serial must be nonempty without surrounding whitespace");
        }
        Ok(Self(value))
    }
}

impl From<Serial> for String {
    fn from(value: Serial) -> Self {
        value.0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceObservation {
    pub(super) block_device: bool,
    pub(super) serial: Option<Serial>,
}
