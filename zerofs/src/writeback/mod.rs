/// Writeback dirty-RAM and dirty-SSD admission.
///
/// RAM and SSD policy owners live in this module. The shared FIFO gate lives
/// in `crate::coordination::admission`.
pub mod admission;
/// Shared durability-barrier primitive.
///
/// The local journaler and the remote scheduler both publish the same progress
/// shape over a `tokio::sync::watch` channel and wait on it with the same loop;
/// only the error vocabulary differs, and that is supplied by
/// [`barrier::BarrierError`].
///
/// The implementation lives in `crate::coordination::sequence`; this module
/// re-exports the old paths until call sites migrate.
pub(crate) mod barrier {
    pub(crate) use crate::coordination::sequence::{
        BarrierError, SequenceBarrier, SequenceProgress,
    };
}
pub mod bootstrap;
pub mod config;
pub mod journal;
pub mod journaler;
pub mod model;
pub mod overlay;
mod payload;
pub mod remote;
pub mod store;
#[cfg(test)]
mod test_util;

#[cfg(unix)]
pub(crate) fn validate_owner_only(
    path: &std::path::Path,
    metadata: &std::fs::Metadata,
    expected_mode: u32,
    label: &str,
) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let mode = metadata.mode() & 0o777;
    if mode != expected_mode {
        anyhow::bail!(
            "{label} {} has mode {mode:o}; expected {expected_mode:o}",
            path.display()
        );
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        anyhow::bail!(
            "{label} {} is not owned by the service user",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn validate_owner_only(
    _path: &std::path::Path,
    _metadata: &std::fs::Metadata,
    _expected_mode: u32,
    _label: &str,
) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod model_contract_tests {
    use super::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
        WritebackStatus,
    };
    use uuid::Uuid;

    #[test]
    fn local_etag_has_a_disjoint_writeback_namespace() {
        let incarnation = Uuid::parse_str("65a4a9b1-68af-44fd-a170-5db569923eb7").unwrap();
        let etag = LocalEtag::new(incarnation, 42);

        assert_eq!(etag.as_str(), "wb:65a4a9b1-68af-44fd-a170-5db569923eb7:42");
    }

    #[test]
    fn mutation_record_round_trips_without_losing_durability_fields() {
        let record = MutationRecord {
            format_version: 1,
            sequence: 7,
            operation_id: Uuid::parse_str("ba211d02-af6d-42ac-9996-62974a4d0c3a").unwrap(),
            path: "segments/00/7".to_owned(),
            kind: MutationKind::Put {
                mode: MutationMode::Create,
                expected_visible_version: None,
                payload_len: 3,
                payload_sha256: [0x5a; 32],
                blob_path: "blobs/00/payload.blob".to_owned(),
            },
            local_etag: LocalEtag::new(Uuid::nil(), 7),
            accepted_at_unix_ms: 1_786_435_200_000,
            remote_predecessor_etag: Some("remote-before".to_owned()),
            remote_result_etag: None,
            fence: FenceClass::ImmutableCreate,
            retry_count: 2,
            last_error: Some("connection reset".to_owned()),
        };

        let encoded = bincode::serialize(&record).unwrap();
        let decoded: MutationRecord = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn journal_identity_and_status_round_trip_as_stable_records() {
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-1".to_owned(),
            backend_endpoint: "sftp://example.com:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x11; 32],
        };
        let status = WritebackStatus {
            accepted_seq: 9,
            local_seq: 8,
            remote_seq: 5,
            dirty_ram_bytes: 4,
            dirty_ram_capacity_bytes: 16,
            dirty_ram_operations: 1,
            dirty_ssd_reserved_bytes: 3,
            dirty_ssd_capacity_bytes: 512,
            dirty_ssd_operations: 2,
            oldest_pending_age_ms: 6,
            local_bytes_completed: 11,
            remote_bytes_completed: 7,
            remote_operations_completed: 5,
            retries: 2,
            terminal_error: None,
        };

        let identity_bytes = bincode::serialize(&identity).unwrap();
        let status_bytes = bincode::serialize(&status).unwrap();
        assert_eq!(
            bincode::deserialize::<JournalIdentity>(&identity_bytes).unwrap(),
            identity
        );
        assert_eq!(
            bincode::deserialize::<WritebackStatus>(&status_bytes).unwrap(),
            status
        );
    }
}
