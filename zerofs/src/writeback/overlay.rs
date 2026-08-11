use crate::writeback::journal::Journal;
use crate::writeback::journaler::LocalCommitObserver;
use crate::writeback::model::{LocalEtag, MutationKind, MutationRecord, Sequence};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{
    Attributes, Extensions, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta,
    ObjectStore, ObjectStoreExt,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisibleVersion {
    Local(LocalEtag),
    Remote {
        e_tag: Option<String>,
        version: Option<String>,
    },
}

#[derive(Clone)]
enum PayloadLocation {
    Memory(Bytes),
    Journal {
        journal: Arc<Journal>,
        sequence: Sequence,
    },
}

#[derive(Clone)]
struct OverlayEntry {
    record: MutationRecord,
    payload: Option<PayloadLocation>,
}

#[derive(Clone)]
pub struct OverlayIndex {
    remote: Arc<dyn ObjectStore>,
    entries: Arc<RwLock<BTreeMap<Path, VecDeque<OverlayEntry>>>>,
}

impl std::fmt::Debug for OverlayIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OverlayIndex")
            .field("remote", &self.remote.to_string())
            .finish_non_exhaustive()
    }
}

impl OverlayIndex {
    pub fn new(remote: Arc<dyn ObjectStore>) -> Self {
        Self {
            remote,
            entries: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn recover(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
    ) -> anyhow::Result<Self> {
        let overlay = Self::new(remote);
        let records = journal.snapshot()?.records;
        let mut entries = BTreeMap::<Path, VecDeque<OverlayEntry>>::new();
        for record in records {
            let path = parse_path(&record.path)?;
            let payload = match record.kind {
                MutationKind::Put { .. } => Some(PayloadLocation::Journal {
                    journal: journal.clone(),
                    sequence: record.sequence,
                }),
                MutationKind::Delete | MutationKind::Copy { .. } | MutationKind::Rename { .. } => {
                    None
                }
            };
            entries
                .entry(path)
                .or_default()
                .push_back(OverlayEntry { record, payload });
        }
        *overlay.entries.write().await = entries;
        Ok(overlay)
    }

    pub async fn install_memory(
        &self,
        record: MutationRecord,
        payload: Bytes,
    ) -> anyhow::Result<()> {
        if !matches!(record.kind, MutationKind::Put { .. }) {
            anyhow::bail!("memory payload requires a put mutation");
        }
        let MutationKind::Put {
            payload_len,
            payload_sha256,
            ..
        } = &record.kind
        else {
            unreachable!("put mutation checked above");
        };
        if *payload_len != payload.len() as u64
            || <[u8; 32]>::from(Sha256::digest(&payload)) != *payload_sha256
        {
            anyhow::bail!("memory payload does not match its mutation record");
        }
        self.install(record, Some(PayloadLocation::Memory(payload)))
            .await
    }

    pub async fn install_delete(&self, record: MutationRecord) -> anyhow::Result<()> {
        if !matches!(record.kind, MutationKind::Delete) {
            anyhow::bail!("delete overlay requires a delete mutation");
        }
        self.install(record, None).await
    }

    async fn install(
        &self,
        record: MutationRecord,
        payload: Option<PayloadLocation>,
    ) -> anyhow::Result<()> {
        let path = parse_path(&record.path)?;
        let mut entries = self.entries.write().await;
        let versions = entries.entry(path).or_default();
        if let Some(previous) = versions.back()
            && record.sequence <= previous.record.sequence
        {
            anyhow::bail!("overlay sequences must increase for each path");
        }
        versions.push_back(OverlayEntry { record, payload });
        Ok(())
    }

    pub async fn mark_local(
        &self,
        sequence: Sequence,
        journal: Arc<Journal>,
    ) -> anyhow::Result<()> {
        let committed = journal
            .mutation(sequence)?
            .ok_or_else(|| anyhow::anyhow!("journal sequence {sequence} does not exist"))?;
        let path = parse_path(&committed.path)?;
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(&path)
            .and_then(|versions| {
                versions
                    .iter_mut()
                    .find(|entry| entry.record.sequence == sequence)
            })
            .ok_or_else(|| anyhow::anyhow!("overlay sequence {sequence} does not exist"))?;
        if matches!(committed.kind, MutationKind::Put { .. }) {
            entry.payload = Some(PayloadLocation::Journal { journal, sequence });
        }
        entry.record = committed;
        Ok(())
    }

    pub async fn remove_remote_prefix(&self, through: Sequence) {
        let mut entries = self.entries.write().await;
        entries.retain(|_, versions| {
            versions.retain(|entry| entry.record.sequence > through);
            !versions.is_empty()
        });
    }

    pub async fn remove_sequence(&self, sequence: Sequence) {
        let mut entries = self.entries.write().await;
        entries.retain(|_, versions| {
            versions.retain(|entry| entry.record.sequence != sequence);
            !versions.is_empty()
        });
    }

    pub async fn visible_version(
        &self,
        location: &Path,
    ) -> object_store::Result<Option<VisibleVersion>> {
        if let Some(entry) = self.visible_entry(location).await {
            return Ok(match entry.record.kind {
                MutationKind::Delete => None,
                _ => Some(VisibleVersion::Local(entry.record.local_etag)),
            });
        }
        match self.remote.head(location).await {
            Ok(meta) => Ok(Some(VisibleVersion::Remote {
                e_tag: meta.e_tag,
                version: meta.version,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    pub async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        Ok(self.get_opts(location, options).await?.meta)
    }

    pub async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Some(entry) = self.visible_entry(location).await else {
            return self.remote.get_opts(location, options).await;
        };
        let Some(payload) = entry.payload else {
            return Err(not_found(location));
        };
        let meta = entry_meta(location.clone(), &entry.record)?;
        options.check_preconditions(&meta)?;
        if let Some(version) = &options.version
            && meta.version.as_ref() != Some(version)
        {
            return Err(not_found(location));
        }
        let bytes = load_payload(payload).await?;
        let range = match options.range {
            Some(range) => range
                .as_range(bytes.len() as u64)
                .map_err(|source| generic_error(format!("invalid get range: {source}")))?,
            None => 0..bytes.len() as u64,
        };
        let body = if options.head {
            Bytes::new()
        } else {
            bytes.slice(range.start as usize..range.end as usize)
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: Attributes::new(),
            extensions: Extensions::new(),
        })
    }

    pub async fn list(&self, prefix: Option<&Path>) -> object_store::Result<Vec<ObjectMeta>> {
        let mut merged = self
            .remote
            .list(prefix)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|meta| (meta.location.clone(), meta))
            .collect::<BTreeMap<_, _>>();
        for (path, entry) in self.visible_entries(prefix).await {
            match entry.record.kind {
                MutationKind::Delete => {
                    merged.remove(&path);
                }
                _ => {
                    merged.insert(path.clone(), entry_meta(path, &entry.record)?);
                }
            }
        }
        Ok(merged.into_values().collect())
    }

    pub async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or(Path::ROOT);
        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        for meta in self.list(Some(&prefix)).await? {
            let remainder = meta
                .location
                .prefix_match(&prefix)
                .expect("list result matches requested prefix")
                .map(|part| part.as_ref().to_owned())
                .collect::<Vec<_>>();
            if remainder.len() <= 1 {
                objects.push(meta);
            } else {
                common_prefixes.insert(prefix.clone().join(remainder[0].as_str()));
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Extensions::new(),
        })
    }

    async fn visible_entry(&self, location: &Path) -> Option<OverlayEntry> {
        self.entries
            .read()
            .await
            .get(location)
            .and_then(|entries| entries.back())
            .cloned()
    }

    async fn visible_entries(&self, prefix: Option<&Path>) -> Vec<(Path, OverlayEntry)> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|(path, _)| prefix.is_none_or(|prefix| path.prefix_matches(prefix)))
            .filter_map(|(path, entries)| {
                entries.back().cloned().map(|entry| (path.clone(), entry))
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct OverlayCommitObserver {
    overlay: OverlayIndex,
    journal: Arc<Journal>,
}

impl OverlayCommitObserver {
    pub fn new(overlay: OverlayIndex, journal: Arc<Journal>) -> Self {
        Self { overlay, journal }
    }
}

#[async_trait::async_trait]
impl LocalCommitObserver for OverlayCommitObserver {
    async fn committed(&self, sequence: Sequence) -> anyhow::Result<()> {
        self.overlay
            .mark_local(sequence, self.journal.clone())
            .await
    }
}

async fn load_payload(payload: PayloadLocation) -> object_store::Result<Bytes> {
    match payload {
        PayloadLocation::Memory(bytes) => Ok(bytes),
        PayloadLocation::Journal { journal, sequence } => {
            tokio::task::spawn_blocking(move || journal.read_blob(sequence).map(Bytes::from))
                .await
                .map_err(|error| generic_error(format!("journal read task failed: {error}")))?
                .map_err(|error| generic_error(format!("journal blob read failed: {error:#}")))
        }
    }
}

fn entry_meta(location: Path, record: &MutationRecord) -> object_store::Result<ObjectMeta> {
    let MutationKind::Put { payload_len, .. } = &record.kind else {
        return Err(not_found(&location));
    };
    let timestamp = i64::try_from(record.accepted_at_unix_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| generic_error("invalid writeback acceptance timestamp".to_owned()))?;
    let local = record.local_etag.as_str().to_owned();
    Ok(ObjectMeta {
        location,
        last_modified: timestamp,
        size: *payload_len,
        e_tag: Some(local.clone()),
        version: Some(local),
    })
}

fn parse_path(path: &str) -> anyhow::Result<Path> {
    Path::parse(path).map_err(|error| anyhow::anyhow!("invalid overlay object path: {error}"))
}

fn not_found(path: &Path) -> object_store::Error {
    object_store::Error::NotFound {
        path: path.to_string(),
        source: "object is hidden by pending writeback mutation".into(),
    }
}

fn generic_error(message: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "ZeroFSWritebackOverlay",
        source: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{OverlayIndex, VisibleVersion};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use uuid::Uuid;

    fn put_record(sequence: u64, path: &str, payload: &[u8]) -> MutationRecord {
        MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::from_u128(0x4000 + sequence as u128),
            path: path.to_owned(),
            kind: MutationKind::Put {
                mode: MutationMode::Overwrite,
                expected_visible_version: None,
                payload_len: payload.len() as u64,
                payload_sha256: Sha256::digest(payload).into(),
                blob_path: String::new(),
            },
            local_etag: LocalEtag::new(Uuid::nil(), sequence),
            accepted_at_unix_ms: 1_786_435_200_000 + sequence,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: 0,
            last_error: None,
        }
    }

    fn delete_record(sequence: u64, path: &str) -> MutationRecord {
        let mut record = put_record(sequence, path, b"");
        record.kind = MutationKind::Delete;
        record
    }

    async fn remote_with(entries: &[(&str, &'static [u8])]) -> Arc<dyn ObjectStore> {
        let remote = Arc::new(InMemory::new());
        for (path, payload) in entries {
            remote
                .put(&Path::from(*path), Bytes::from_static(payload).into())
                .await
                .unwrap();
        }
        remote
    }

    #[tokio::test]
    async fn pending_put_and_delete_override_remote_get_head_and_list() {
        let remote = remote_with(&[("tree/a", b"old-a"), ("tree/b", b"remote-b")]).await;
        let overlay = OverlayIndex::new(remote);
        overlay
            .install_delete(delete_record(1, "tree/a"))
            .await
            .unwrap();
        overlay
            .install_memory(
                put_record(2, "tree/c", b"local-c"),
                Bytes::from_static(b"local-c"),
            )
            .await
            .unwrap();

        assert!(overlay.get(&Path::from("tree/a")).await.is_err());
        assert!(overlay.head(&Path::from("tree/a")).await.is_err());
        assert_eq!(
            overlay
                .get(&Path::from("tree/c"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"local-c")
        );
        let paths = overlay
            .list(Some(&Path::from("tree")))
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["tree/b", "tree/c"]);
    }

    #[tokio::test]
    async fn mismatched_memory_payload_is_rejected_before_becoming_visible() {
        let overlay = OverlayIndex::new(remote_with(&[]).await);

        let error = overlay
            .install_memory(
                put_record(1, "object", b"expected"),
                Bytes::from_static(b"different"),
            )
            .await
            .unwrap_err();

        assert!(format!("{error:#}").contains("does not match"));
        assert!(overlay.get(&Path::from("object")).await.is_err());
    }

    #[tokio::test]
    async fn local_get_honors_ranges_etags_and_head_without_payload() {
        let overlay = OverlayIndex::new(remote_with(&[]).await);
        overlay
            .install_memory(
                put_record(1, "object", b"0123456789"),
                Bytes::from_static(b"0123456789"),
            )
            .await
            .unwrap();
        let etag = "wb:00000000-0000-0000-0000-000000000000:1";

        let ranged = overlay
            .get_opts(
                &Path::from("object"),
                GetOptions {
                    range: Some(GetRange::Bounded(2..6)),
                    if_match: Some(etag.to_owned()),
                    ..GetOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(ranged.range, 2..6);
        assert_eq!(ranged.bytes().await.unwrap(), Bytes::from_static(b"2345"));

        let suffix = overlay
            .get_opts(
                &Path::from("object"),
                GetOptions {
                    range: Some(GetRange::Suffix(3)),
                    ..GetOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(suffix.bytes().await.unwrap(), Bytes::from_static(b"789"));
        assert!(
            overlay
                .get_opts(
                    &Path::from("object"),
                    GetOptions {
                        if_match: Some("stale".to_owned()),
                        ..GetOptions::default()
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(overlay.head(&Path::from("object")).await.unwrap().size, 10);
    }

    #[tokio::test]
    async fn recovered_ssd_blob_is_visible_without_remote_data() {
        let temp = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-a".to_owned(),
                    backend_endpoint: "sftp://example.com:23".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "sftp".to_owned(),
                    encryption_key_identity_sha256: [0x55; 32],
                },
            )
            .unwrap(),
        );
        journal
            .commit_put(put_record(1, "recovered", b"ssd-data"), b"ssd-data")
            .unwrap();
        let overlay = OverlayIndex::recover(remote_with(&[]).await, journal.clone())
            .await
            .unwrap();

        assert_eq!(
            overlay
                .get(&Path::from("recovered"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"ssd-data")
        );
    }

    #[tokio::test]
    async fn visibility_has_no_gap_during_memory_ssd_and_remote_handoffs() {
        let remote = remote_with(&[]).await;
        let overlay = OverlayIndex::new(remote.clone());
        let record = put_record(1, "object", b"payload");
        overlay
            .install_memory(record.clone(), Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_eq!(
            overlay
                .visible_version(&Path::from("object"))
                .await
                .unwrap(),
            Some(VisibleVersion::Local(record.local_etag.clone()))
        );

        let temp = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-a".to_owned(),
                    backend_endpoint: "sftp://example.com:23".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "sftp".to_owned(),
                    encryption_key_identity_sha256: [0x66; 32],
                },
            )
            .unwrap(),
        );
        let committed = journal.commit_put(record, b"payload").unwrap();
        overlay
            .mark_local(committed.sequence, journal)
            .await
            .unwrap();
        assert_eq!(
            overlay
                .get(&Path::from("object"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"payload")
        );

        remote
            .put(&Path::from("object"), Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        overlay.remove_remote_prefix(1).await;
        assert_eq!(
            overlay
                .get(&Path::from("object"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"payload")
        );
    }

    #[tokio::test]
    async fn delimiter_listing_merges_remote_and_pending_paths_without_tombstoned_prefixes() {
        let overlay = OverlayIndex::new(
            remote_with(&[("root/direct", b"remote"), ("root/old/file", b"old")]).await,
        );
        overlay
            .install_delete(delete_record(1, "root/old/file"))
            .await
            .unwrap();
        overlay
            .install_memory(
                put_record(2, "root/new/file", b"new"),
                Bytes::from_static(b"new"),
            )
            .await
            .unwrap();

        let result = overlay
            .list_with_delimiter(Some(&Path::from("root")))
            .await
            .unwrap();

        assert_eq!(
            result
                .objects
                .into_iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
            vec!["root/direct"]
        );
        assert_eq!(
            result
                .common_prefixes
                .into_iter()
                .map(|path| path.to_string())
                .collect::<Vec<_>>(),
            vec!["root/new"]
        );
    }

    proptest::proptest! {
        #[test]
        fn newest_pending_mutation_matches_a_literal_namespace_model(
            operations in proptest::collection::vec(
                (0_u8..8, proptest::bool::ANY, proptest::collection::vec(proptest::num::u8::ANY, 0..24)),
                1..80,
            )
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let overlay = OverlayIndex::new(remote_with(&[]).await);
                let mut expected = BTreeMap::<String, Option<Bytes>>::new();
                for (index, (key, is_put, payload)) in operations.into_iter().enumerate() {
                    let sequence = index as u64 + 1;
                    let path = format!("tree/k{key}");
                    if is_put {
                        let payload = Bytes::from(payload);
                        overlay
                            .install_memory(put_record(sequence, &path, &payload), payload.clone())
                            .await
                            .unwrap();
                        expected.insert(path, Some(payload));
                    } else {
                        overlay
                            .install_delete(delete_record(sequence, &path))
                            .await
                            .unwrap();
                        expected.insert(path, None);
                    }
                }

                let actual_paths = overlay
                    .list(Some(&Path::from("tree")))
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|meta| meta.location.to_string())
                    .collect::<Vec<_>>();
                let expected_paths = expected
                    .iter()
                    .filter_map(|(path, value)| value.as_ref().map(|_| path.clone()))
                    .collect::<Vec<_>>();
                proptest::prop_assert_eq!(actual_paths, expected_paths);
                for (path, expected_payload) in expected {
                    let actual = overlay.get(&Path::from(path.as_str())).await;
                    match expected_payload {
                        Some(payload) => {
                            proptest::prop_assert_eq!(actual.unwrap().bytes().await.unwrap(), payload)
                        }
                        None => proptest::prop_assert!(actual.is_err()),
                    }
                }
                Ok(())
            })?;
        }
    }
}
