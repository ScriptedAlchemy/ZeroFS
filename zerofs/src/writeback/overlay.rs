use crate::writeback::journal::Journal;
use crate::writeback::journaler::LocalCommitObserver;
use crate::writeback::model::{LocalEtag, MutationKind, MutationRecord, Sequence};
use crate::writeback::payload::VerifiedPayload;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{
    Attributes, Extensions, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta,
    ObjectStore, ObjectStoreExt,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverlayEffect {
    Put,
    Delete,
}

#[derive(Clone)]
struct OverlayEntry {
    record: MutationRecord,
    effect: OverlayEffect,
    payload: Option<PayloadLocation>,
}

#[derive(Default)]
struct OverlayState {
    entries: BTreeMap<Path, VecDeque<OverlayEntry>>,
    paths_by_sequence: BTreeMap<Sequence, BTreeSet<Path>>,
}

const BLOB_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Bounded cache of fully verified spilled blobs. A journal blob is read and
/// SHA-256-verified as a whole, so without this every ranged read of the same
/// locally-durable object would re-read and re-hash the entire blob.
#[derive(Default)]
struct BlobCache {
    entries: VecDeque<(Sequence, Bytes)>,
    total_bytes: usize,
}

impl BlobCache {
    fn get(&mut self, sequence: Sequence) -> Option<Bytes> {
        let index = self
            .entries
            .iter()
            .position(|(cached, _)| *cached == sequence)?;
        let entry = self.entries.remove(index).expect("cache index in bounds");
        let bytes = entry.1.clone();
        self.entries.push_back(entry);
        Some(bytes)
    }

    fn insert(&mut self, sequence: Sequence, bytes: Bytes) {
        if bytes.len() > BLOB_CACHE_MAX_BYTES
            || self.entries.iter().any(|(cached, _)| *cached == sequence)
        {
            return;
        }
        self.total_bytes += bytes.len();
        self.entries.push_back((sequence, bytes));
        while self.total_bytes > BLOB_CACHE_MAX_BYTES {
            let (_, evicted) = self
                .entries
                .pop_front()
                .expect("cached bytes imply entries");
            self.total_bytes -= evicted.len();
        }
    }

    fn retain(&mut self, keep: impl Fn(Sequence) -> bool) {
        self.entries.retain(|(sequence, _)| keep(*sequence));
        self.total_bytes = self.entries.iter().map(|(_, bytes)| bytes.len()).sum();
    }
}

#[derive(Clone)]
pub struct OverlayIndex {
    remote: Arc<dyn ObjectStore>,
    state: Arc<RwLock<OverlayState>>,
    blob_cache: Arc<StdMutex<BlobCache>>,
    #[cfg(test)]
    cleanup_path_visits: Arc<std::sync::atomic::AtomicUsize>,
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
            state: Arc::new(RwLock::new(OverlayState::default())),
            blob_cache: Arc::new(StdMutex::new(BlobCache::default())),
            #[cfg(test)]
            cleanup_path_visits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub async fn recover(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
    ) -> anyhow::Result<Self> {
        let overlay = Self::new(remote);
        let records = journal.snapshot()?.records;
        let mut state = OverlayState::default();
        for record in records {
            let path = parse_path(&record.path)?;
            let payload = match record.kind {
                MutationKind::Put { .. }
                | MutationKind::Copy { .. }
                | MutationKind::Rename { .. } => Some(PayloadLocation::Journal {
                    journal: journal.clone(),
                    sequence: record.sequence,
                }),
                MutationKind::Delete => None,
            };
            let effect = if matches!(record.kind, MutationKind::Delete) {
                OverlayEffect::Delete
            } else {
                OverlayEffect::Put
            };
            install_locked(&mut state, path, record.clone(), effect, payload)?;
            if let MutationKind::Rename { source, .. } = &record.kind {
                install_locked(
                    &mut state,
                    parse_path(source)?,
                    record,
                    OverlayEffect::Delete,
                    None,
                )?;
            }
        }
        *overlay.state.write().await = state;
        Ok(overlay)
    }

    pub async fn install_memory(
        &self,
        record: MutationRecord,
        payload: Bytes,
    ) -> anyhow::Result<()> {
        self.install_verified_memory(record, VerifiedPayload::new(payload))
            .await
    }

    pub(crate) async fn install_verified_memory(
        &self,
        record: MutationRecord,
        payload: VerifiedPayload,
    ) -> anyhow::Result<()> {
        if !matches!(record.kind, MutationKind::Put { .. }) {
            anyhow::bail!("memory payload requires a put mutation");
        }
        if !payload.matches_record(&record) {
            anyhow::bail!("memory payload does not match its mutation record");
        }
        self.install(
            record,
            OverlayEffect::Put,
            Some(PayloadLocation::Memory(payload.into_bytes())),
        )
        .await
    }

    pub async fn install_delete(&self, record: MutationRecord) -> anyhow::Result<()> {
        if !matches!(record.kind, MutationKind::Delete) {
            anyhow::bail!("delete overlay requires a delete mutation");
        }
        self.install(record, OverlayEffect::Delete, None).await
    }

    pub async fn install_copy(&self, record: MutationRecord, payload: Bytes) -> anyhow::Result<()> {
        self.install_verified_copy(record, VerifiedPayload::new(payload))
            .await
    }

    pub(crate) async fn install_verified_copy(
        &self,
        record: MutationRecord,
        payload: VerifiedPayload,
    ) -> anyhow::Result<()> {
        if !matches!(record.kind, MutationKind::Copy { .. }) {
            anyhow::bail!("copy overlay requires a copy mutation");
        }
        validate_verified_payload(&record, &payload)?;
        self.install(
            record,
            OverlayEffect::Put,
            Some(PayloadLocation::Memory(payload.into_bytes())),
        )
        .await
    }

    pub async fn install_rename(
        &self,
        record: MutationRecord,
        payload: Bytes,
    ) -> anyhow::Result<()> {
        self.install_verified_rename(record, VerifiedPayload::new(payload))
            .await
    }

    pub(crate) async fn install_verified_rename(
        &self,
        record: MutationRecord,
        payload: VerifiedPayload,
    ) -> anyhow::Result<()> {
        let MutationKind::Rename { source, .. } = &record.kind else {
            anyhow::bail!("rename overlay requires a rename mutation");
        };
        validate_verified_payload(&record, &payload)?;
        let target = parse_path(&record.path)?;
        let source = parse_path(source)?;
        let mut state = self.state.write().await;
        install_locked(
            &mut state,
            target,
            record.clone(),
            OverlayEffect::Put,
            Some(PayloadLocation::Memory(payload.into_bytes())),
        )?;
        if let Err(error) = install_locked(
            &mut state,
            source,
            record.clone(),
            OverlayEffect::Delete,
            None,
        ) {
            remove_sequence_locked(&mut state, record.sequence);
            return Err(error);
        }
        Ok(())
    }

    async fn install(
        &self,
        record: MutationRecord,
        effect: OverlayEffect,
        payload: Option<PayloadLocation>,
    ) -> anyhow::Result<()> {
        let path = parse_path(&record.path)?;
        let mut state = self.state.write().await;
        install_locked(&mut state, path, record, effect, payload)
    }

    pub async fn mark_local(
        &self,
        sequence: Sequence,
        journal: Arc<Journal>,
    ) -> anyhow::Result<()> {
        let committed = journal
            .mutation(sequence)?
            .ok_or_else(|| anyhow::anyhow!("journal sequence {sequence} does not exist"))?;
        self.mark_local_batch(std::slice::from_ref(&committed), journal)
            .await
    }

    /// Switch every overlay entry of a committed publication batch onto the
    /// journal payload under a single write-lock acquisition. The committed
    /// records are supplied by the publication path, so no journal read is
    /// needed here.
    pub async fn mark_local_batch(
        &self,
        records: &[MutationRecord],
        journal: Arc<Journal>,
    ) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut state = self.state.write().await;
        for committed in records {
            let sequence = committed.sequence;
            let mut matched = false;
            let paths = state
                .paths_by_sequence
                .get(&sequence)
                .cloned()
                .unwrap_or_default();
            for path in paths {
                let Some(versions) = state.entries.get_mut(&path) else {
                    continue;
                };
                for entry in versions
                    .iter_mut()
                    .filter(|entry| entry.record.sequence == sequence)
                {
                    if entry.effect == OverlayEffect::Put {
                        entry.payload = Some(PayloadLocation::Journal {
                            journal: journal.clone(),
                            sequence,
                        });
                    }
                    entry.record = committed.clone();
                    matched = true;
                }
            }
            if !matched {
                anyhow::bail!("overlay sequence {sequence} does not exist");
            }
        }
        Ok(())
    }

    pub async fn remove_remote_prefix(&self, through: Sequence) {
        let mut state = self.state.write().await;
        let affected = state
            .paths_by_sequence
            .range(..=through)
            .flat_map(|(_, paths)| paths.iter().cloned())
            .collect::<BTreeSet<_>>();
        for path in affected {
            #[cfg(test)]
            self.cleanup_path_visits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let remove_path = state.entries.get_mut(&path).is_some_and(|versions| {
                versions.retain(|entry| entry.record.sequence > through);
                versions.is_empty()
            });
            if remove_path {
                state.entries.remove(&path);
            }
        }
        state
            .paths_by_sequence
            .retain(|sequence, _| *sequence > through);
        drop(state);
        self.blob_cache().retain(|sequence| sequence > through);
    }

    pub async fn remove_sequence(&self, sequence: Sequence) {
        let mut state = self.state.write().await;
        remove_sequence_locked(&mut state, sequence);
        drop(state);
        self.blob_cache().retain(|cached| cached != sequence);
    }

    pub async fn visible_version(
        &self,
        location: &Path,
    ) -> object_store::Result<Option<VisibleVersion>> {
        if let Some(entry) = self.visible_entry(location).await {
            return Ok(match entry.effect {
                OverlayEffect::Delete => None,
                OverlayEffect::Put => Some(VisibleVersion::Local(entry.record.local_etag)),
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

    pub(crate) async fn has_visible_local_object(&self, location: &Path) -> bool {
        self.visible_entry(location)
            .await
            .is_some_and(|entry| matches!(entry.effect, OverlayEffect::Put))
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
        let range = match options.range {
            Some(range) => range
                .as_range(meta.size)
                .map_err(|source| generic_error(format!("invalid get range: {source}")))?,
            None => 0..meta.size,
        };
        let body = if options.head {
            Bytes::new()
        } else {
            self.load_payload(payload)
                .await?
                .slice(range.start as usize..range.end as usize)
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
        // Snapshot the overlay first so this list linearizes before any remote
        // publication/removal handoff that may run while the backend streams.
        let visible = self.visible_entries(prefix).await;
        let mut merged = self
            .remote
            .list(prefix)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|meta| (meta.location.clone(), meta))
            .collect::<BTreeMap<_, _>>();
        for (path, entry) in visible {
            match entry.effect {
                OverlayEffect::Delete => {
                    merged.remove(&path);
                }
                OverlayEffect::Put => {
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

    async fn load_payload(&self, payload: PayloadLocation) -> object_store::Result<Bytes> {
        match payload {
            PayloadLocation::Memory(bytes) => Ok(bytes),
            PayloadLocation::Journal { journal, sequence } => {
                if let Some(bytes) = self.blob_cache().get(sequence) {
                    return Ok(bytes);
                }
                let bytes = tokio::task::spawn_blocking(move || {
                    journal.read_blob(sequence).map(Bytes::from)
                })
                .await
                .map_err(|error| generic_error(format!("journal read task failed: {error}")))?
                .map_err(|error| generic_error(format!("journal blob read failed: {error:#}")))?;
                self.blob_cache().insert(sequence, bytes.clone());
                Ok(bytes)
            }
        }
    }

    fn blob_cache(&self) -> std::sync::MutexGuard<'_, BlobCache> {
        self.blob_cache.lock().expect("overlay blob cache poisoned")
    }

    async fn visible_entry(&self, location: &Path) -> Option<OverlayEntry> {
        self.state
            .read()
            .await
            .entries
            .get(location)
            .and_then(|entries| entries.back())
            .cloned()
    }

    async fn visible_entries(&self, prefix: Option<&Path>) -> Vec<(Path, OverlayEntry)> {
        self.state
            .read()
            .await
            .entries
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
    async fn committed_batch(&self, records: &[MutationRecord]) -> anyhow::Result<()> {
        self.overlay
            .mark_local_batch(records, self.journal.clone())
            .await
    }
}

fn entry_meta(location: Path, record: &MutationRecord) -> object_store::Result<ObjectMeta> {
    let Some((payload_len, _)) = record.payload() else {
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
        size: payload_len,
        e_tag: Some(local.clone()),
        version: Some(local),
    })
}

fn validate_verified_payload(
    record: &MutationRecord,
    payload: &VerifiedPayload,
) -> anyhow::Result<()> {
    if !payload.matches_record(record) {
        anyhow::bail!("memory payload does not match its mutation record");
    }
    Ok(())
}

fn install_locked(
    state: &mut OverlayState,
    path: Path,
    record: MutationRecord,
    effect: OverlayEffect,
    payload: Option<PayloadLocation>,
) -> anyhow::Result<()> {
    let sequence = record.sequence;
    let versions = state.entries.entry(path.clone()).or_default();
    if let Some(previous) = versions.back()
        && record.sequence <= previous.record.sequence
    {
        anyhow::bail!("overlay sequences must increase for each path");
    }
    versions.push_back(OverlayEntry {
        record,
        effect,
        payload,
    });
    state
        .paths_by_sequence
        .entry(sequence)
        .or_default()
        .insert(path);
    Ok(())
}

fn remove_sequence_locked(state: &mut OverlayState, sequence: Sequence) {
    let paths = state
        .paths_by_sequence
        .remove(&sequence)
        .unwrap_or_default();
    for path in paths {
        let remove_path = state.entries.get_mut(&path).is_some_and(|versions| {
            versions.retain(|entry| entry.record.sequence != sequence);
            versions.is_empty()
        });
        if remove_path {
            state.entries.remove(&path);
        }
    }
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
    use crate::writeback::model::{FenceClass, JournalIdentity, MutationMode, MutationRecord};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::{StreamExt, stream::BoxStream};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::collections::BTreeMap;
    use std::fmt::{self, Display, Formatter};
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct PausedListStore {
        inner: Arc<InMemory>,
        captured: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Display for PausedListStore {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            write!(formatter, "PausedListStore")
        }
    }

    #[async_trait]
    impl ObjectStore for PausedListStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let inner = self.inner.clone();
            let prefix = prefix.cloned();
            let captured = self.captured.clone();
            let release = self.release.clone();
            futures::stream::once(async move {
                let snapshot = inner
                    .list(prefix.as_ref())
                    .collect::<Vec<object_store::Result<ObjectMeta>>>()
                    .await;
                captured.notify_one();
                release.notified().await;
                snapshot
            })
            .flat_map(futures::stream::iter)
            .boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn paused_list_store(inner: Arc<InMemory>) -> (Arc<dyn ObjectStore>, Arc<Notify>, Arc<Notify>) {
        let captured = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        (
            Arc::new(PausedListStore {
                inner,
                captured: captured.clone(),
                release: release.clone(),
            }),
            captured,
            release,
        )
    }

    fn put_record(sequence: u64, path: &str, payload: &[u8]) -> MutationRecord {
        crate::writeback::test_util::put_record(
            sequence,
            path,
            payload,
            MutationMode::Overwrite,
            FenceClass::Fence,
            0x4000,
            1_786_435_200_000,
        )
    }

    fn delete_record(sequence: u64, path: &str) -> MutationRecord {
        crate::writeback::test_util::delete_record(
            sequence,
            path,
            FenceClass::Fence,
            0x4000,
            1_786_435_200_000,
        )
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
    async fn list_keeps_a_put_visible_while_remote_publication_removes_its_overlay() {
        let inner = Arc::new(InMemory::new());
        let (remote, captured, release) = paused_list_store(inner.clone());
        let overlay = OverlayIndex::new(remote);
        overlay
            .install_memory(
                put_record(1, "tree/object", b"local"),
                Bytes::from_static(b"local"),
            )
            .await
            .unwrap();

        let listing = tokio::spawn({
            let overlay = overlay.clone();
            async move { overlay.list(Some(&Path::from("tree"))).await.unwrap() }
        });
        captured.notified().await;
        inner
            .put(
                &Path::from("tree/object"),
                Bytes::from_static(b"local").into(),
            )
            .await
            .unwrap();
        overlay.remove_remote_prefix(1).await;
        release.notify_one();

        let paths = listing
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["tree/object"]);
    }

    #[tokio::test]
    async fn list_keeps_a_delete_hidden_while_remote_removal_clears_its_tombstone() {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("tree/object"),
                Bytes::from_static(b"remote").into(),
            )
            .await
            .unwrap();
        let (remote, captured, release) = paused_list_store(inner.clone());
        let overlay = OverlayIndex::new(remote);
        overlay
            .install_delete(delete_record(1, "tree/object"))
            .await
            .unwrap();

        let listing = tokio::spawn({
            let overlay = overlay.clone();
            async move { overlay.list(Some(&Path::from("tree"))).await.unwrap() }
        });
        captured.notified().await;
        inner.delete(&Path::from("tree/object")).await.unwrap();
        overlay.remove_remote_prefix(1).await;
        release.notify_one();

        assert!(listing.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn remote_cleanup_visits_only_paths_changed_by_the_completed_sequence() {
        let overlay = OverlayIndex::new(remote_with(&[]).await);
        for sequence in 1..=1_000 {
            overlay
                .install_memory(
                    put_record(sequence, &format!("tree/object-{sequence}"), b"x"),
                    Bytes::from_static(b"x"),
                )
                .await
                .unwrap();
        }
        overlay
            .cleanup_path_visits
            .store(0, std::sync::atomic::Ordering::SeqCst);

        overlay.remove_remote_prefix(1).await;

        assert_eq!(
            overlay
                .cleanup_path_visits
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "remote completion must not rescan every unrelated overlay path"
        );
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

    fn remove_journal_blobs(root: &std::path::Path) {
        let blobs = root.join("blobs");
        let mut pending = vec![blobs];
        while let Some(directory) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&directory) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
    }

    fn test_journal(root: &std::path::Path) -> Arc<Journal> {
        Arc::new(
            Journal::open(
                root,
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-a".to_owned(),
                    backend_endpoint: "sftp://example.com:23".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "sftp".to_owned(),
                    encryption_key_identity_sha256: [0x77; 32],
                },
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn head_of_spilled_blob_does_not_read_the_journal_payload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = test_journal(&root);
        let record = put_record(1, "object", b"spilled-data");
        let overlay = OverlayIndex::new(remote_with(&[]).await);
        overlay
            .install_memory(record.clone(), Bytes::from_static(b"spilled-data"))
            .await
            .unwrap();
        let committed = journal.commit_put(record, b"spilled-data").unwrap();
        overlay
            .mark_local(committed.sequence, journal)
            .await
            .unwrap();

        // Metadata answers must not depend on re-reading the payload bytes.
        remove_journal_blobs(&root);
        assert_eq!(
            overlay.head(&Path::from("object")).await.unwrap().size,
            b"spilled-data".len() as u64
        );
    }

    #[tokio::test]
    async fn repeated_ranged_reads_of_a_spilled_blob_reuse_the_verified_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = test_journal(&root);
        let record = put_record(1, "object", b"spilled-data");
        let overlay = OverlayIndex::new(remote_with(&[]).await);
        overlay
            .install_memory(record.clone(), Bytes::from_static(b"spilled-data"))
            .await
            .unwrap();
        let committed = journal.commit_put(record, b"spilled-data").unwrap();
        overlay
            .mark_local(committed.sequence, journal)
            .await
            .unwrap();

        let first = overlay
            .get_opts(
                &Path::from("object"),
                GetOptions {
                    range: Some(GetRange::Bounded(0..7)),
                    ..GetOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(first.bytes().await.unwrap(), Bytes::from_static(b"spilled"));

        // The first read verified the whole blob; later ranges are served from
        // the cache without another full journal read + hash.
        remove_journal_blobs(&root);
        let second = overlay
            .get_opts(
                &Path::from("object"),
                GetOptions {
                    range: Some(GetRange::Bounded(8..12)),
                    ..GetOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(second.bytes().await.unwrap(), Bytes::from_static(b"data"));

        // Publication removes the overlay entry and drops the cached bytes.
        overlay.remove_remote_prefix(1).await;
        assert!(overlay.blob_cache().entries.is_empty());
        assert_eq!(overlay.blob_cache().total_bytes, 0);
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
