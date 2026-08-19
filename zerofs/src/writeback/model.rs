use object_store::path::Path;
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

/// Derive replay ordering from the persisted operation contract, not from an
/// object key alone. Only an explicit create-only PUT to a canonical immutable
/// database object may preupload across an earlier ordering fence. Multipart
/// completion is persisted as `Overwrite`, so it deliberately cannot qualify
/// even when its destination resembles a compacted SST.
pub(crate) fn classify_mutation_fence(
    path: &str,
    kind: &MutationKind,
    database_prefix: &str,
) -> FenceClass {
    if !matches!(
        kind,
        MutationKind::Put {
            mode: MutationMode::Create,
            ..
        }
    ) {
        return FenceClass::Fence;
    }

    let (Ok(location), Ok(database_prefix)) = (Path::parse(path), Path::parse(database_prefix))
    else {
        return FenceClass::Fence;
    };
    let Some(suffix) = location.prefix_match(&database_prefix) else {
        return FenceClass::Fence;
    };
    let suffix = suffix
        .map(|part| part.as_ref().to_owned())
        .collect::<Vec<_>>();

    let is_segment = suffix.len() == 4
        && suffix[0] == "segments"
        && crate::segment::Segid::from_object_key(&suffix.join("/")).is_some();
    let is_sst = match suffix.as_slice() {
        [directory, filename] if directory == "wal" => canonical_wal_filename(filename),
        [directory, filename] if directory == "compacted" => canonical_compacted_filename(filename),
        _ => false,
    };

    if is_segment || is_sst {
        FenceClass::ImmutableCreate
    } else {
        FenceClass::Fence
    }
}

fn canonical_wal_filename(filename: &str) -> bool {
    filename
        .strip_suffix(".sst")
        .and_then(|stem| stem.parse::<u64>().ok().map(|id| (stem, id)))
        .is_some_and(|(stem, id)| stem == format!("{id:020}"))
}

fn canonical_compacted_filename(filename: &str) -> bool {
    const CROCKFORD_BASE32: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    filename.strip_suffix(".sst").is_some_and(|stem| {
        stem.len() == 26
            && stem.as_bytes()[0].is_ascii_digit()
            && stem.as_bytes()[0] <= b'7'
            && stem.bytes().all(|byte| CROCKFORD_BASE32.contains(&byte))
    })
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
    /// Bounded allowance for each persisted object-store version string.
    /// Normal backends use short HTTP ETags, but keeping three 4 KiB slots
    /// covers the accepted version, remote predecessor, and remote result
    /// without making the charge depend on when retry/publication fields grow.
    pub(crate) const MAX_PERSISTED_VERSION_BYTES: usize = 4_096;
    /// A redb mutation entry stores the fixed-width u64 sequence key, a u32
    /// value-end offset, and a four-byte leaf header. Charging the full header
    /// to every entry is deliberately conservative for packed leaves.
    const MUTATION_TABLE_ENTRY_OVERHEAD_BYTES: u64 = 8 + 4 + 4;
    /// The remote-version table duplicates a bounded result ETag behind a
    /// variable path key. Account for the path, encoded `(sequence, ETag)`
    /// value, both variable-entry offsets, and a full leaf header.
    const REMOTE_VERSION_TABLE_FIXED_BYTES: u64 = 8 + 8 + 4 + 4 + 4;
    /// The longest blob reference the journal can mint: a batch container
    /// slice, `blobs/{shard}/{first:016x}-{last:016x}.blobs#{offset}+{len}`
    /// with both slice numbers at their `u64::MAX` decimal width. It bounds
    /// the pre-container whole-file form too, so one constant still covers
    /// every reservation.
    ///
    /// This is 90 bytes against the 47 the whole-file form needed, and every
    /// pending mutation reserves against it whether or not its own reference
    /// is that long. The 43-byte difference is noise beside any real payload,
    /// but a metadata-heavy workload reserves per record rather than per byte,
    /// so its effective dirty-SSD capacity drops by roughly that much per
    /// pending record. Sizing the constant to the actual reference instead
    /// would make the reservation depend on which batch a record landed in,
    /// which is not known at admission.
    const MAX_BLOB_PATH: &str = "blobs/ff/ffffffffffffffff-ffffffffffffffff.blobs#18446744073709551615+18446744073709551615";
    const MAX_LOCAL_ETAG: &str = "wb:ffffffff-ffff-ffff-ffff-ffffffffffff:18446744073709551615";
    /// Every accepted mutation occupies one SSD operation slot. Recovery
    /// reseeds admission from the pending-record count, never payload length.
    pub const SSD_RESERVATION_OPERATIONS: u64 = 1;

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

    /// Stable SSD admission reservation for a pending mutation.
    ///
    /// The estimate covers the external blob, a conservative serialized
    /// mutation record (including retry/version growth), and its redb table
    /// entry. It depends only on fields known before foreground admission and
    /// retained in the journal, so acceptance, restart recovery, metrics, and
    /// remote release reconstruct the same value without changing the journal
    /// format.
    pub fn ssd_reservation_estimate(
        path: &str,
        source: Option<&str>,
        payload_len: u64,
    ) -> bincode::Result<u64> {
        let kind = match source {
            Some(source) => MutationKind::Rename {
                source: source.to_owned(),
                mode: MutationMode::Update,
                payload_len,
                payload_sha256: [u8::MAX; 32],
                blob_path: Self::MAX_BLOB_PATH.to_owned(),
            },
            None => MutationKind::Put {
                mode: MutationMode::Update,
                expected_visible_version: None,
                payload_len,
                payload_sha256: [u8::MAX; 32],
                blob_path: Self::MAX_BLOB_PATH.to_owned(),
            },
        };
        let record = Self {
            format_version: u32::MAX,
            sequence: u64::MAX,
            operation_id: Uuid::nil(),
            path: path.to_owned(),
            kind,
            local_etag: LocalEtag::new(Uuid::nil(), u64::MAX),
            accepted_at_unix_ms: u64::MAX,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: u32::MAX,
            last_error: None,
        };
        record.ssd_reservation_bytes()
    }

    /// Compatibility wrapper for metadata-only foreground admission.
    pub fn metadata_ssd_reservation(path: &str) -> bincode::Result<u64> {
        Self {
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
        }
        .ssd_reservation_bytes()
    }

    /// Operation slots charged for this record. Always one; payload length
    /// never substitutes for the operation count.
    pub fn ssd_reservation_operations(&self) -> u64 {
        Self::SSD_RESERVATION_OPERATIONS
    }

    /// Reconstruct the reservation owned by a persisted record.
    ///
    /// New mutations are bounded before sequence allocation. Journals created
    /// by older binaries may contain larger version metadata, so recovery and
    /// release expand the reservation to the record's actual logical footprint
    /// instead of rejecting an otherwise readable journal.
    pub fn ssd_reservation_bytes(&self) -> bincode::Result<u64> {
        let payload_len = self.payload().map_or(0, |(payload_len, _)| payload_len);
        let mut normalized = self.clone();
        normalized.local_etag.0 =
            Self::larger_value(&normalized.local_etag.0, Self::MAX_LOCAL_ETAG);
        if let Some(blob_path) = normalized.blob_path_mut()
            && blob_path.len() < Self::MAX_BLOB_PATH.len()
        {
            *blob_path = Self::MAX_BLOB_PATH.to_owned();
        }
        if let MutationKind::Put {
            expected_visible_version,
            ..
        } = &mut normalized.kind
        {
            *expected_visible_version = Some(Self::larger_value(
                expected_visible_version.as_deref().unwrap_or_default(),
                &"v".repeat(Self::MAX_PERSISTED_VERSION_BYTES),
            ));
        }
        normalized.remote_predecessor_etag = Some(Self::larger_value(
            normalized
                .remote_predecessor_etag
                .as_deref()
                .unwrap_or_default(),
            &"p".repeat(Self::MAX_PERSISTED_VERSION_BYTES),
        ));
        normalized.remote_result_etag = Some(Self::larger_value(
            normalized.remote_result_etag.as_deref().unwrap_or_default(),
            &"r".repeat(Self::MAX_PERSISTED_VERSION_BYTES),
        ));
        normalized.last_error = Some(Self::larger_value(
            normalized.last_error.as_deref().unwrap_or_default(),
            &"\u{10ffff}".repeat(2_048),
        ));
        let future_result_len = normalized
            .remote_result_etag
            .as_ref()
            .map_or(0, String::len) as u64;
        let future_remote_version_entry = [
            self.path.len() as u64,
            future_result_len,
            Self::REMOTE_VERSION_TABLE_FIXED_BYTES,
        ]
        .into_iter()
        .try_fold(0_u64, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or_else(|| Box::new(bincode::ErrorKind::SizeLimit))
        })?;
        [
            payload_len,
            bincode::serialized_size(&normalized)?,
            Self::MUTATION_TABLE_ENTRY_OVERHEAD_BYTES,
            future_remote_version_entry,
        ]
        .into_iter()
        .try_fold(0_u64, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or_else(|| Box::new(bincode::ErrorKind::SizeLimit))
        })
    }

    fn larger_value(actual: &str, bound: &str) -> String {
        if actual.len() > bound.len() {
            actual.to_owned()
        } else {
            bound.to_owned()
        }
    }

    pub fn validate_persisted_version_field(
        field: &'static str,
        value: Option<&str>,
    ) -> Result<(), PersistedMetadataError> {
        let Some(value) = value else {
            return Ok(());
        };
        if value.len() > Self::MAX_PERSISTED_VERSION_BYTES {
            return Err(PersistedMetadataError::VersionFieldTooLarge {
                field,
                actual: value.len(),
                maximum: Self::MAX_PERSISTED_VERSION_BYTES,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PersistedMetadataError {
    #[error("persisted {field} is {actual} bytes; maximum is {maximum} bytes")]
    VersionFieldTooLarge {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
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
    pub dirty_ssd_reserved_bytes: u64,
    pub dirty_ssd_capacity_bytes: u64,
    pub dirty_ssd_operations: u64,
    pub oldest_pending_age_ms: u64,
    pub local_bytes_completed: u64,
    pub remote_bytes_completed: u64,
    pub remote_operations_completed: u64,
    pub retries: u64,
    pub terminal_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{FenceClass, LocalEtag, MutationKind, MutationMode, MutationRecord};
    use uuid::Uuid;

    fn record(kind: MutationKind) -> MutationRecord {
        MutationRecord {
            format_version: 1,
            sequence: 7,
            operation_id: Uuid::from_u128(0x1234),
            path: "target/object".to_owned(),
            kind,
            local_etag: LocalEtag::new(Uuid::from_u128(0x5678), 7),
            accepted_at_unix_ms: 1_700_000_000_000,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: 0,
            last_error: None,
        }
    }

    #[test]
    fn payload_disk_charge_covers_blob_serialized_record_and_redb_entry() {
        let payload_len = 1;
        let payload_sha256 = [0x42; 32];
        let blob_path = "blobs/07/00000000-0000-0000-0000-000000001234.blob".to_owned();
        let records = [
            record(MutationKind::Put {
                mode: MutationMode::Overwrite,
                expected_visible_version: None,
                payload_len,
                payload_sha256,
                blob_path: blob_path.clone(),
            }),
            record(MutationKind::Copy {
                source: "source/object".to_owned(),
                mode: MutationMode::Create,
                payload_len,
                payload_sha256,
                blob_path: blob_path.clone(),
            }),
            record(MutationKind::Rename {
                source: "source/object".to_owned(),
                mode: MutationMode::Overwrite,
                payload_len,
                payload_sha256,
                blob_path,
            }),
        ];

        for record in records {
            let serialized_record = bincode::serialized_size(&record).unwrap();
            let redb_sequence_key_and_value_offset = 12;
            let minimum_persisted_bytes =
                payload_len + serialized_record + redb_sequence_key_and_value_offset;

            assert!(
                record.ssd_reservation_bytes().unwrap() >= minimum_persisted_bytes,
                "payload charge must cover its blob and mutation-table entry: {:?}",
                record.kind
            );
        }
    }

    #[test]
    fn payload_disk_charge_is_stable_as_retry_metadata_grows() {
        let mut record = record(MutationKind::Put {
            mode: MutationMode::Update,
            expected_visible_version: Some("expected-version".to_owned()),
            payload_len: 1,
            payload_sha256: [0x42; 32],
            blob_path: "blobs/07/00000000-0000-0000-0000-000000001234.blob".to_owned(),
        });
        record.remote_predecessor_etag = Some("remote-predecessor".to_owned());
        let accepted_charge = record.ssd_reservation_bytes().unwrap();

        record.retry_count = 17;
        record.last_error = Some("\u{10ffff}".repeat(2_048));

        assert_eq!(record.ssd_reservation_bytes().unwrap(), accepted_charge);
    }

    #[test]
    fn legacy_payload_charge_expands_for_metadata_larger_than_the_foreground_allowance() {
        let mut record = record(MutationKind::Put {
            mode: MutationMode::Update,
            expected_visible_version: Some("e".repeat(5_000)),
            payload_len: 1,
            payload_sha256: [0x42; 32],
            blob_path: "blobs/07/00000000-0000-0000-0000-000000001234.blob".to_owned(),
        });
        record.remote_predecessor_etag = Some("p".repeat(5_000));
        record.remote_result_etag = Some("r".repeat(5_000));
        record.last_error = Some("\u{10ffff}".repeat(2_048));

        let actual_persisted_bytes = 1
            + bincode::serialized_size(&record).unwrap()
            + MutationRecord::MUTATION_TABLE_ENTRY_OVERHEAD_BYTES;
        assert!(record.ssd_reservation_bytes().unwrap() >= actual_persisted_bytes);
    }

    #[test]
    fn reservation_covers_a_maximum_result_etag_in_both_durable_tables() {
        let maximum_etag = "e".repeat(MutationRecord::MAX_PERSISTED_VERSION_BYTES);
        let mut record = record(MutationKind::Put {
            mode: MutationMode::Update,
            expected_visible_version: Some("v".repeat(MutationRecord::MAX_PERSISTED_VERSION_BYTES)),
            payload_len: 1,
            payload_sha256: [0x42; 32],
            blob_path: "blobs/07/00000000-0000-0000-0000-000000001234.blob".to_owned(),
        });
        record.remote_predecessor_etag =
            Some("p".repeat(MutationRecord::MAX_PERSISTED_VERSION_BYTES));
        record.remote_result_etag = Some(maximum_etag.clone());
        record.last_error = Some("\u{10ffff}".repeat(2_048));
        let remote_version_entry = bincode::serialized_size(&(record.sequence, maximum_etag))
            .unwrap()
            + record.path.len() as u64
            + 12;
        let minimum_reserved = 1
            + bincode::serialized_size(&record).unwrap()
            + MutationRecord::MUTATION_TABLE_ENTRY_OVERHEAD_BYTES
            + remote_version_entry;

        assert!(record.ssd_reservation_bytes().unwrap() >= minimum_reserved);
    }

    #[test]
    fn legacy_reservation_keeps_oversized_versions_and_future_growth_allowances() {
        let mut pending = record(MutationKind::Put {
            mode: MutationMode::Overwrite,
            expected_visible_version: Some("e".repeat(50_000)),
            payload_len: 1,
            payload_sha256: [0x42; 32],
            blob_path: "blobs/07/00000000-0000-0000-0000-000000001234.blob".to_owned(),
        });
        pending.remote_predecessor_etag = Some("p".repeat(50_000));
        let reserved_before_remote = pending.ssd_reservation_bytes().unwrap();

        let maximum_result = "r".repeat(MutationRecord::MAX_PERSISTED_VERSION_BYTES);
        pending.remote_result_etag = Some(maximum_result.clone());
        pending.last_error = Some("\u{10ffff}".repeat(2_048));
        let remote_version_entry = pending.path.len() as u64
            + bincode::serialized_size(&(pending.sequence, maximum_result)).unwrap()
            + 12;
        let post_mark_footprint = 1
            + bincode::serialized_size(&pending).unwrap()
            + MutationRecord::MUTATION_TABLE_ENTRY_OVERHEAD_BYTES
            + remote_version_entry;

        assert!(reserved_before_remote >= post_mark_footprint);
    }

    #[test]
    fn recovery_seeds_one_operation_per_pending_record() {
        let payload = record(MutationKind::Put {
            mode: MutationMode::Update,
            expected_visible_version: None,
            payload_len: 1_048_576,
            payload_sha256: [0x11; 32],
            blob_path: "blobs/07/00000000-0000-0000-0000-000000001234.blob".to_owned(),
        });
        let metadata = record(MutationKind::Delete);
        assert_eq!(
            payload.ssd_reservation_operations(),
            MutationRecord::SSD_RESERVATION_OPERATIONS
        );
        assert_eq!(
            metadata.ssd_reservation_operations(),
            MutationRecord::SSD_RESERVATION_OPERATIONS
        );
        assert_eq!(MutationRecord::SSD_RESERVATION_OPERATIONS, 1);
        assert_ne!(
            payload.ssd_reservation_bytes().unwrap(),
            metadata.ssd_reservation_bytes().unwrap()
        );
    }
}
