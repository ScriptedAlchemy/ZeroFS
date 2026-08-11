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
    },
    Rename {
        source: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocalEtag(String);

impl LocalEtag {
    pub fn new(journal_incarnation: Uuid, sequence: Sequence) -> Self {
        Self(format!("wb:{journal_incarnation}:{sequence}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WritebackStatus {
    pub accepted_seq: Sequence,
    pub local_seq: Sequence,
    pub remote_seq: Sequence,
    pub dirty_ram_bytes: u64,
    pub dirty_ram_operations: u64,
    pub dirty_ssd_bytes: u64,
    pub dirty_ssd_operations: u64,
    pub oldest_pending_age_ms: u64,
    pub remote_bytes_completed: u64,
    pub remote_operations_completed: u64,
    pub retries: u64,
    pub terminal_error: Option<String>,
}
