pub mod admission;
pub mod config;
pub mod journal;
pub mod model;

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
            dirty_ram_operations: 1,
            dirty_ssd_bytes: 3,
            dirty_ssd_operations: 2,
            oldest_pending_age_ms: 6,
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
