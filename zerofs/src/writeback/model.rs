use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Sequence = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalIdentity {
    pub format_version: u32,
    pub bucket_id: String,
    pub backend_endpoint: String,
    pub database_prefix: String,
    pub backend_kind: String,
    pub encryption_key_identity_sha256: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationMode {
    Overwrite,
    Create,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationKind {
    Put {
        mode: MutationMode,
        expected_visible_version: Option<String>,
        payload_len: u64,
        payload_sha256: [u8; 32],
        blob_path: String,
    },
    Delete,
    Copy {
        source: String,
        mode: MutationMode,
        payload_len: u64,
        payload_sha256: [u8; 32],
        blob_path: String,
    },
    Rename {
        source: String,
        mode: MutationMode,
        payload_len: u64,
        payload_sha256: [u8; 32],
        blob_path: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FenceClass {
    ImmutableCreate,
    Fence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationRecord {
    pub format_version: u32,
    pub sequence: Sequence,
    pub operation_id: Uuid,
    pub path: String,
    pub kind: MutationKind,
    pub local_etag: LocalEtag,
    pub accepted_at_unix_ms: u64,
    pub remote_predecessor_etag: Option<String>,
    pub remote_result_etag: Option<String>,
    pub fence: FenceClass,
    pub retry_count: u32,
    pub last_error: Option<String>,
}

impl MutationRecord {
    /// Maximum UTF-8 byte length of the persisted retry text. The journal
    /// truncates remote errors to 2,048 Unicode scalar values, each of which
    /// can occupy four UTF-8 bytes. The small suffix covers bincode's option
    /// and string-length encoding.
    const MAX_RETRY_ERROR_ENCODED_BYTES: u64 = 2_048 * 4 + 16;

    pub fn blob_path(&self) -> Option<&str> {
        match &self.kind {
            MutationKind::Put { blob_path, .. }
            | MutationKind::Copy { blob_path, .. }
            | MutationKind::Rename { blob_path, .. } => Some(blob_path),
            MutationKind::Delete => None,
        }
    }

    pub fn payload(&self) -> Option<(u64, [u8; 32])> {
        match &self.kind {
            MutationKind::Put {
                payload_len,
                payload_sha256,
                ..
            }
            | MutationKind::Copy {
                payload_len,
                payload_sha256,
                ..
            }
            | MutationKind::Rename {
                payload_len,
                payload_sha256,
                ..
            } => Some((*payload_len, *payload_sha256)),
            MutationKind::Delete => None,
        }
    }

    pub fn blob_path_mut(&mut self) -> Option<&mut String> {
        match &mut self.kind {
            MutationKind::Put { blob_path, .. }
            | MutationKind::Copy { blob_path, .. }
            | MutationKind::Rename { blob_path, .. } => Some(blob_path),
            MutationKind::Delete => None,
        }
    }

    /// Logical SSD charge for a pending mutation without a payload blob.
    ///
    /// Sequence and UUID values have fixed-width bincode encodings. Using the
    /// longest possible local ETag makes this independent of admission order,
    /// so the caller can reserve space before allocating a sequence.
    pub fn metadata_disk_charge(path: &str) -> bincode::Result<u64> {
        let record = Self {
            format_version: u32::MAX,
            sequence: u64::MAX,
            operation_id: Uuid::nil(),
            path: path.to_owned(),
            kind: MutationKind::Delete,
            local_etag: LocalEtag::new(Uuid::nil(), u64::MAX),
            accepted_at_unix_ms: u64::MAX,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: u32::MAX,
            last_error: None,
        };
        bincode::serialized_size(&record).and_then(|encoded| {
            encoded
                .checked_add(Self::MAX_RETRY_ERROR_ENCODED_BYTES)
                .ok_or_else(|| Box::new(bincode::ErrorKind::SizeLimit))
        })
    }

    pub fn disk_charge_bytes(&self) -> bincode::Result<u64> {
        match self.payload() {
            Some((payload_len, _)) => Ok(payload_len),
            None => Self::metadata_disk_charge(&self.path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocalEtag(String);

impl LocalEtag {
    pub fn new(journal_incarnation: Uuid, sequence: Sequence) -> Self {
        Self(format!("wb:{journal_incarnation}:{sequence}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn sequence_from_str(value: &str) -> Option<Sequence> {
        let (namespace_and_incarnation, sequence) = value.rsplit_once(':')?;
        let incarnation = namespace_and_incarnation.strip_prefix("wb:")?;
        Uuid::parse_str(incarnation).ok()?;
        sequence.parse().ok()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WritebackStatus {
    pub accepted_seq: Sequence,
    pub local_seq: Sequence,
    pub remote_seq: Sequence,
    pub dirty_ram_bytes: u64,
    pub dirty_ram_capacity_bytes: u64,
    pub dirty_ram_operations: u64,
    pub dirty_ssd_bytes: u64,
    pub dirty_ssd_capacity_bytes: u64,
    pub dirty_ssd_operations: u64,
    pub oldest_pending_age_ms: u64,
    pub local_bytes_completed: u64,
    pub remote_bytes_completed: u64,
    pub remote_operations_completed: u64,
    pub retries: u64,
    pub terminal_error: Option<String>,
}
