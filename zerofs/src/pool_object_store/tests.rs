use super::{PoolMember, PoolObjectStore};
use crate::segment::Segid;
use crate::segment_store::{ConditionalMultipartCreate, GeneratedSegmentCreate};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result, UpdateVersion,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

fn segment(counter: u64) -> Path {
    Path::from(format!("db/{}", Segid::new(41, counter).object_key()))
}

fn create_options() -> PutOptions {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(GeneratedSegmentCreate);
    options
}

async fn create(store: &PoolObjectStore, path: &Path, data: &'static [u8]) -> PutResult {
    store
        .put_opts(path, Bytes::from_static(data).into(), create_options())
        .await
        .unwrap()
}

async fn read(store: &dyn ObjectStore, path: &Path) -> Bytes {
    store.get(path).await.unwrap().bytes().await.unwrap()
}

async fn usage(store: &PoolObjectStore, id: &str) -> (u64, u64, u64) {
    let member = store
        .status()
        .await
        .unwrap()
        .members
        .into_iter()
        .find(|member| member.id == id)
        .unwrap();
    (member.used_bytes, member.reserved_bytes, member.objects)
}

async fn placement(store: &PoolObjectStore, path: &Path) -> String {
    store
        .placements()
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.location == *path)
        .unwrap()
        .member_id
}

async fn multipart(store: &PoolObjectStore, path: &Path, size: u64) -> Box<dyn MultipartUpload> {
    let capability = ConditionalMultipartCreate::for_payload_len(size);
    let mut options = PutMultipartOptions::default();
    options.extensions.insert(GeneratedSegmentCreate);
    options.extensions.insert(capability.clone());
    let upload = store.put_multipart_opts(path, options).await.unwrap();
    assert!(capability.is_acknowledged());
    upload
}

struct Fixture {
    catalog: tempfile::TempDir,
    authority: Arc<InMemory>,
    first: Arc<FaultStore>,
    second: Arc<FaultStore>,
    capacity: u64,
}

impl Fixture {
    fn new(capacity: u64) -> Self {
        Self {
            catalog: tempfile::tempdir().unwrap(),
            authority: Arc::new(InMemory::new()),
            first: FaultStore::new(),
            second: FaultStore::new(),
            capacity,
        }
    }

    fn member(&self, first: bool) -> PoolMember {
        let (id, store): (&str, Arc<dyn ObjectStore>) = if first {
            ("hdd-a", self.first.clone())
        } else {
            ("hdd-b", self.second.clone())
        };
        PoolMember {
            id: id.to_owned(),
            identity: format!("memory://{id}"),
            store,
            capacity_bytes: self.capacity,
            local_path: None,
            min_free_bytes: 0,
        }
    }

    fn members(&self) -> Vec<PoolMember> {
        vec![self.member(true), self.member(false)]
    }

    async fn initialize(&self) -> PoolObjectStore {
        PoolObjectStore::initialize(
            self.authority.clone(),
            Path::from("db"),
            self.catalog.path().join("catalog"),
            self.members(),
        )
        .await
        .unwrap()
    }

    async fn reopen(&self) -> Result<PoolObjectStore> {
        PoolObjectStore::open(
            self.authority.clone(),
            Path::from("db"),
            self.catalog.path().join("catalog"),
            self.members(),
        )
        .await
    }
}

#[derive(Debug, Default)]
struct FaultControl {
    offline: AtomicBool,
    fail_deletes: AtomicBool,
    corrupt_payload_len: AtomicU64,
    gate_payload_len: AtomicU64,
    entered: Notify,
    release: Notify,
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<InMemory>,
    control: Arc<FaultControl>,
}

impl FaultStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemory::new()),
            control: Arc::new(FaultControl::default()),
        })
    }

    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "PoolFaultStore",
            source: "injected pool member failure".into(),
        }
    }

    fn check_online(&self) -> Result<()> {
        if self.control.offline.load(Ordering::SeqCst) {
            Err(Self::injected())
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PoolFaultStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.check_online()?;
        let length = payload.content_length() as u64;
        if length != 0
            && self
                .control
                .gate_payload_len
                .compare_exchange(length, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check_online()?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.check_online()?;
        let head = options.head;
        let mut result = self.inner.get_opts(location, options).await?;
        let corrupt_length = self.control.corrupt_payload_len.load(Ordering::SeqCst);
        if !head && corrupt_length != 0 && result.meta.size == corrupt_length {
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = std::mem::take(&mut result.extensions);
            let payload = result
                .into_stream()
                .map_ok(|bytes| {
                    let mut bytes = bytes.to_vec();
                    if let Some(first) = bytes.first_mut() {
                        *first ^= 1;
                    }
                    Bytes::from(bytes)
                })
                .boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(payload),
                meta,
                range,
                attributes,
                extensions,
            });
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let control = self.control.clone();
        locations
            .map(move |location| {
                let inner = inner.clone();
                let control = control.clone();
                async move {
                    let location = location?;
                    if control.offline.load(Ordering::SeqCst)
                        || control.fail_deletes.load(Ordering::SeqCst)
                    {
                        return Err(Self::injected());
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffered(8)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if let Err(error) = self.check_online() {
            return futures::stream::once(async { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.check_online()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.check_online()?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn metadata_stays_on_authority_while_segments_fill_members_in_order() {
    let fixture = Fixture::new(8);
    let pool = fixture.initialize().await;
    let metadata = Path::from("db/manifest/current");
    pool.put(&metadata, Bytes::from_static(b"metadata").into())
        .await
        .unwrap();
    create(&pool, &segment(1), b"12345678").await;
    create(&pool, &segment(2), b"abcd").await;

    assert_eq!(
        read(fixture.authority.as_ref(), &metadata).await,
        "metadata"
    );
    assert!(matches!(
        fixture.authority.head(&segment(1)).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(placement(&pool, &segment(1)).await, "hdd-a");
    assert_eq!(placement(&pool, &segment(2)).await, "hdd-b");
    assert_eq!(usage(&pool, "hdd-a").await, (8, 0, 1));
    assert_eq!(usage(&pool, "hdd-b").await, (4, 0, 1));
}

#[tokio::test]
async fn adding_and_reordering_members_does_not_move_existing_segments() {
    let fixture = Fixture::new(8);
    let pool = PoolObjectStore::initialize(
        fixture.authority.clone(),
        Path::from("db"),
        fixture.catalog.path().join("catalog"),
        vec![fixture.member(true)],
    )
    .await
    .unwrap();
    create(&pool, &segment(1), b"12345678").await;
    let before = pool.head(&segment(1)).await.unwrap();
    drop(pool);

    let pool = PoolObjectStore::open(
        fixture.authority.clone(),
        Path::from("db"),
        fixture.catalog.path().join("catalog"),
        vec![fixture.member(false), fixture.member(true)],
    )
    .await
    .unwrap();
    assert_eq!(placement(&pool, &segment(1)).await, "hdd-a");
    assert_eq!(pool.head(&segment(1)).await.unwrap().e_tag, before.e_tag);
    assert_eq!(read(&pool, &segment(1)).await, "12345678");
    create(&pool, &segment(2), b"second").await;
    assert_eq!(placement(&pool, &segment(2)).await, "hdd-b");
}

#[tokio::test]
async fn only_canonical_segments_within_the_database_prefix_are_pooled() {
    let fixture = Fixture::new(8);
    let pool = fixture.initialize().await;
    for path in [
        Path::from(format!("other/{}", Segid::new(41, 1).object_key())),
        Path::from("db/segments/not-a-segment"),
        Path::from("db/wal/00000000000000000001.sst"),
    ] {
        pool.put(&path, Bytes::from_static(b"metadata").into())
            .await
            .unwrap();
        assert_eq!(read(fixture.authority.as_ref(), &path).await, "metadata");
    }
    assert_eq!(pool.status().await.unwrap().mapped_objects, 0);
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
}

#[tokio::test]
async fn conditional_create_and_update_preserve_the_visible_generation() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    let path = segment(1);
    let original = create(&pool, &path, b"old").await;
    assert!(matches!(
        pool.put_opts(&path, Bytes::from_static(b"bad").into(), create_options())
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    let stale = PutMode::Update(UpdateVersion {
        e_tag: Some("stale-generation".to_owned()),
        version: None,
    });
    assert!(matches!(
        pool.put_opts(&path, Bytes::from_static(b"bad").into(), stale.into())
            .await,
        Err(object_store::Error::Precondition { .. })
    ));
    assert_eq!(read(&pool, &path).await, "old");

    let update = PutMode::Update(UpdateVersion {
        e_tag: original.e_tag.clone(),
        version: None,
    });
    let updated = pool
        .put_opts(&path, Bytes::from_static(b"new").into(), update.into())
        .await
        .unwrap();
    assert_ne!(updated.e_tag, original.e_tag);
    assert_eq!(read(&pool, &path).await, "new");
    assert_eq!(pool.status().await.unwrap().mapped_objects, 1);
}

#[tokio::test]
async fn multipart_reserves_bytes_before_parts_and_publishes_only_on_completion() {
    let fixture = Fixture::new(6);
    let pool = fixture.initialize().await;
    let path = segment(1);
    let mut upload = multipart(&pool, &path, 6).await;
    assert_eq!(usage(&pool, "hdd-a").await, (0, 6, 0));
    upload
        .put_part(Bytes::from_static(b"abc").into())
        .await
        .unwrap();
    upload
        .put_part(Bytes::from_static(b"def").into())
        .await
        .unwrap();
    assert!(matches!(
        pool.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    upload.complete().await.unwrap();
    assert_eq!(read(&pool, &path).await, "abcdef");
    assert_eq!(usage(&pool, "hdd-a").await, (6, 0, 1));
}

#[tokio::test]
async fn pending_uploads_cannot_overcommit_capacity_and_abort_releases_it() {
    let fixture = Fixture::new(6);
    let pool = fixture.initialize().await;
    let mut first = multipart(&pool, &segment(1), 6).await;
    let mut second = multipart(&pool, &segment(2), 6).await;
    assert_eq!(usage(&pool, "hdd-a").await, (0, 6, 0));
    assert_eq!(usage(&pool, "hdd-b").await, (0, 6, 0));
    assert!(
        pool.put_opts(
            &segment(3),
            Bytes::from_static(b"x").into(),
            create_options()
        )
        .await
        .is_err()
    );
    first.abort().await.unwrap();
    second.abort().await.unwrap();
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
    assert_eq!(usage(&pool, "hdd-b").await, (0, 0, 0));
    create(&pool, &segment(3), b"123456").await;
}

#[tokio::test]
async fn a_short_multipart_payload_is_never_published() {
    let fixture = Fixture::new(8);
    let pool = fixture.initialize().await;
    let path = segment(1);
    let mut upload = multipart(&pool, &path, 6).await;
    upload
        .put_part(Bytes::from_static(b"abc").into())
        .await
        .unwrap();
    assert!(upload.complete().await.is_err());
    let _ = upload.abort().await;
    assert!(matches!(
        pool.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(pool.status().await.unwrap().mapped_objects, 0);
}

#[tokio::test]
async fn migration_preserves_etag_creation_time_and_conditional_range_reads() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    let path = segment(1);
    create(&pool, &path, b"abcdef").await;
    let before = pool.head(&path).await.unwrap();
    pool.migrate(&path, "hdd-b").await.unwrap();
    let after = pool.head(&path).await.unwrap();
    assert_eq!(placement(&pool, &path).await, "hdd-b");
    assert_eq!(after.location, path);
    assert_eq!(after.size, before.size);
    assert_eq!(after.e_tag, before.e_tag);
    assert_eq!(after.last_modified, before.last_modified);
    assert_eq!(after.version, before.version);

    for (range, expected) in [
        (GetRange::Bounded(1..5), "bcde"),
        (GetRange::Offset(2), "cdef"),
        (GetRange::Suffix(3), "def"),
    ] {
        let result = pool
            .get_opts(
                &path,
                GetOptions {
                    if_match: before.e_tag.clone(),
                    range: Some(range),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.meta.location, path);
        assert_eq!(result.meta.size, 6);
        assert_eq!(result.bytes().await.unwrap(), expected);
    }
    assert!(matches!(
        pool.get_opts(
            &path,
            GetOptions {
                if_match: Some("stale-generation".to_owned()),
                ..Default::default()
            }
        )
        .await,
        Err(object_store::Error::Precondition { .. })
    ));
    assert!(matches!(
        pool.get_opts(
            &path,
            GetOptions {
                if_none_match: before.e_tag,
                ..Default::default()
            }
        )
        .await,
        Err(object_store::Error::NotModified { .. })
    ));
    assert!(matches!(
        pool.get_opts(
            &path,
            GetOptions {
                version: Some("unknown-version".to_owned()),
                ..Default::default()
            }
        )
        .await,
        Err(object_store::Error::NotSupported { .. })
    ));
}

#[tokio::test]
async fn corrupt_migration_keeps_the_source_and_charges_failed_cleanup_until_restart() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    let path = segment(1);
    create(&pool, &path, b"abcdef").await;
    fixture
        .second
        .control
        .corrupt_payload_len
        .store(6, Ordering::SeqCst);
    fixture
        .second
        .control
        .fail_deletes
        .store(true, Ordering::SeqCst);
    assert!(pool.migrate(&path, "hdd-b").await.is_err());
    assert_eq!(placement(&pool, &path).await, "hdd-a");
    assert_eq!(read(&pool, &path).await, "abcdef");
    assert!(usage(&pool, "hdd-b").await.1 >= 6);
    fixture
        .second
        .control
        .corrupt_payload_len
        .store(0, Ordering::SeqCst);
    fixture
        .second
        .control
        .fail_deletes
        .store(false, Ordering::SeqCst);
    drop(pool);

    let pool = fixture.reopen().await.unwrap();
    assert_eq!(read(&pool, &path).await, "abcdef");
    assert_eq!(usage(&pool, "hdd-b").await, (0, 0, 0));
    assert_eq!(pool.status().await.unwrap().pending_operations, 0);
}

#[tokio::test]
async fn migration_committed_before_source_cleanup_survives_restart() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    let path = segment(1);
    create(&pool, &path, b"abcdef").await;
    let before = pool.head(&path).await.unwrap();
    fixture
        .first
        .control
        .fail_deletes
        .store(true, Ordering::SeqCst);
    let _ = pool.migrate(&path, "hdd-b").await;
    assert_eq!(placement(&pool, &path).await, "hdd-b");
    assert_eq!(read(&pool, &path).await, "abcdef");
    assert_eq!(pool.head(&path).await.unwrap().e_tag, before.e_tag);
    fixture
        .first
        .control
        .fail_deletes
        .store(false, Ordering::SeqCst);
    drop(pool);

    let pool = fixture.reopen().await.unwrap();
    assert_eq!(placement(&pool, &path).await, "hdd-b");
    assert_eq!(read(&pool, &path).await, "abcdef");
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
    assert_eq!(usage(&pool, "hdd-b").await, (6, 0, 1));
    assert_eq!(pool.status().await.unwrap().pending_operations, 0);
}

#[tokio::test]
async fn unavailable_member_does_not_block_other_reads_listing_or_restart() {
    let fixture = Fixture::new(6);
    let pool = fixture.initialize().await;
    create(&pool, &segment(1), b"local!").await;
    create(&pool, &segment(2), b"remote").await;
    fixture.second.control.offline.store(true, Ordering::SeqCst);
    assert_eq!(read(&pool, &segment(1)).await, "local!");
    assert!(pool.get(&segment(2)).await.is_err());
    let listed: Vec<_> = pool
        .list(Some(&Path::from("db/segments")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    drop(pool);

    let pool = fixture.reopen().await.unwrap();
    assert_eq!(read(&pool, &segment(1)).await, "local!");
    assert!(pool.get(&segment(2)).await.is_err());
}

#[tokio::test]
async fn a_failed_mapped_read_never_falls_back_to_stale_authority_bytes() {
    let fixture = Fixture::new(16);
    let pool = fixture.initialize().await;
    let path = segment(1);
    create(&pool, &path, b"fresh").await;
    fixture
        .authority
        .put(&path, Bytes::from_static(b"stale").into())
        .await
        .unwrap();
    fixture.first.control.offline.store(true, Ordering::SeqCst);
    assert!(pool.get(&path).await.is_err());
    fixture.first.control.offline.store(false, Ordering::SeqCst);
    assert_eq!(read(&pool, &path).await, "fresh");
}

#[tokio::test]
async fn legacy_authority_segments_can_be_read_and_migrated_without_changing_identity() {
    let fixture = Fixture::new(32);
    let path = segment(1);
    fixture
        .authority
        .put(&path, Bytes::from_static(b"legacy").into())
        .await
        .unwrap();
    let original = fixture.authority.head(&path).await.unwrap();
    let pool = fixture.initialize().await;
    assert_eq!(read(&pool, &path).await, "legacy");
    pool.migrate(&path, "hdd-b").await.unwrap();
    assert_eq!(placement(&pool, &path).await, "hdd-b");
    let migrated = pool.head(&path).await.unwrap();
    assert_eq!(migrated.e_tag, original.e_tag);
    assert_eq!(migrated.last_modified, original.last_modified);
    assert_eq!(read(&pool, &path).await, "legacy");
    drop(pool);

    let pool = fixture.reopen().await.unwrap();
    assert_eq!(read(&pool, &path).await, "legacy");
}

#[tokio::test]
async fn listing_and_delimiters_merge_legacy_and_pool_objects_without_duplicates() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    for counter in [1, 2] {
        create(&pool, &segment(counter), b"data").await;
    }
    fixture
        .authority
        .put(&segment(3), Bytes::from_static(b"legacy").into())
        .await
        .unwrap();
    fixture
        .authority
        .put(&segment(1), Bytes::from_static(b"stale").into())
        .await
        .unwrap();
    let prefix = Path::from("db/segments");
    let listed: Vec<_> = pool.list(Some(&prefix)).try_collect().await.unwrap();
    let locations: BTreeSet<_> = listed.iter().map(|entry| entry.location.clone()).collect();
    let expected: BTreeSet<_> = [segment(1), segment(2), segment(3)].into_iter().collect();
    assert_eq!(locations, expected);
    assert_eq!(listed.len(), 3);
    assert_eq!(
        listed
            .iter()
            .find(|entry| entry.location == segment(1))
            .unwrap()
            .size,
        4
    );
    let expected_prefixes: BTreeSet<_> = expected
        .iter()
        .map(|path| {
            Path::from(
                path.as_ref()
                    .split('/')
                    .take(3)
                    .collect::<Vec<_>>()
                    .join("/"),
            )
        })
        .collect();
    let delimited = pool.list_with_delimiter(Some(&prefix)).await.unwrap();
    assert!(delimited.objects.is_empty());
    assert_eq!(
        delimited
            .common_prefixes
            .into_iter()
            .collect::<BTreeSet<_>>(),
        expected_prefixes
    );
}

#[tokio::test]
async fn a_missing_catalog_cannot_be_recreated_over_existing_pool_authority() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    create(&pool, &segment(1), b"data").await;
    drop(pool);
    let missing = fixture.catalog.path().join("missing");
    assert!(
        PoolObjectStore::open(
            fixture.authority.clone(),
            Path::from("db"),
            missing.clone(),
            fixture.members(),
        )
        .await
        .is_err()
    );
    assert!(
        PoolObjectStore::initialize(
            fixture.authority.clone(),
            Path::from("db"),
            missing,
            fixture.members(),
        )
        .await
        .is_err()
    );
    let pool = fixture.reopen().await.unwrap();
    assert_eq!(read(&pool, &segment(1)).await, "data");
}

#[tokio::test]
async fn removing_or_rebinding_a_member_with_live_data_fails_closed() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    create(&pool, &segment(1), b"data").await;
    drop(pool);
    assert!(
        PoolObjectStore::open(
            fixture.authority.clone(),
            Path::from("db"),
            fixture.catalog.path().join("catalog"),
            vec![fixture.member(false)],
        )
        .await
        .is_err()
    );
    let mut rebound = fixture.members();
    rebound[0].identity = "memory://replacement".to_owned();
    assert!(
        PoolObjectStore::open(
            fixture.authority.clone(),
            Path::from("db"),
            fixture.catalog.path().join("catalog"),
            rebound,
        )
        .await
        .is_err()
    );
    let pool = fixture.reopen().await.unwrap();
    assert_eq!(read(&pool, &segment(1)).await, "data");
}

#[tokio::test]
async fn a_replaced_member_root_cannot_silently_be_treated_as_empty_storage() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    create(&pool, &segment(1), b"data").await;
    drop(pool);
    let mut replaced = fixture.members();
    replaced[0].store = Arc::new(InMemory::new());
    if let Ok(pool) = PoolObjectStore::open(
        fixture.authority.clone(),
        Path::from("db"),
        fixture.catalog.path().join("catalog"),
        replaced,
    )
    .await
    {
        assert!(pool.get(&segment(1)).await.is_err());
        if pool
            .put_opts(
                &segment(2),
                Bytes::from_static(b"new").into(),
                create_options(),
            )
            .await
            .is_ok()
        {
            assert_eq!(placement(&pool, &segment(2)).await, "hdd-b");
        }
        assert!(pool.get(&segment(1)).await.is_err());
    }
}

#[tokio::test]
async fn the_catalog_excludes_a_second_writer_until_the_first_closes() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    let second = tokio::time::timeout(Duration::from_secs(5), fixture.reopen())
        .await
        .expect("a busy catalog must fail promptly");
    assert!(second.is_err());
    drop(pool);
    fixture.reopen().await.unwrap();
}

#[tokio::test]
async fn cancelled_member_put_is_recovered_without_publishing_or_leaking_quota() {
    let fixture = Fixture::new(6);
    let pool = fixture.initialize().await;
    fixture
        .first
        .control
        .gate_payload_len
        .store(6, Ordering::SeqCst);
    let writer = pool.clone();
    let task = tokio::spawn(async move {
        writer
            .put_opts(
                &segment(1),
                Bytes::from_static(b"abcdef").into(),
                create_options(),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.first.control.entered.notified(),
    )
    .await
    .expect("the member PUT should be reached after reserving capacity");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(pool.status().await.unwrap().mapped_objects, 0);
    assert!(
        fixture.reopen().await.is_err(),
        "an in-flight physical writer must keep the catalog exclusively owned"
    );
    fixture.first.control.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if pool.status().await.unwrap().pending_operations == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the cancelled PUT's physical owner must settle and clean its intent");
    assert_eq!(pool.status().await.unwrap().mapped_objects, 0);
    drop(pool);

    let pool = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(pool) = fixture.reopen().await {
                break pool;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the finished physical owner must release the catalog");
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
    assert_eq!(pool.status().await.unwrap().pending_operations, 0);
    create(&pool, &segment(2), b"abcdef").await;
    assert_eq!(usage(&pool, "hdd-a").await, (6, 0, 1));
}

#[tokio::test]
async fn copied_catalog_cannot_open_a_second_owner_at_a_different_directory() {
    let fixture = Fixture::new(32);
    let pool = fixture.initialize().await;
    let path = segment(1);
    create(&pool, &path, b"original-data").await;
    drop(pool);

    // Copy only after closing the original database, so this is a valid
    // catalog copy rather than a torn snapshot or a malformed file.
    let copied_directory = tempfile::tempdir().unwrap();
    // Make this a valid private catalog directory before testing its identity.
    // Some hosted environments create temporary directories with mode 0755.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(
            copied_directory.path(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }
    std::fs::copy(
        fixture
            .catalog
            .path()
            .join("catalog")
            .join(super::CATALOG_FILE_NAME),
        copied_directory.path().join(super::CATALOG_FILE_NAME),
    )
    .unwrap();

    let error = PoolObjectStore::open(
        fixture.authority.clone(),
        Path::from("db"),
        copied_directory.path().to_path_buf(),
        fixture.members(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("bound to a different catalog directory"),
        "a valid copy must fail on its directory identity: {error}"
    );

    let original = fixture.reopen().await.unwrap();
    assert_eq!(read(&original, &path).await, "original-data");
    assert_eq!(placement(&original, &path).await, "hdd-a");
}

#[tokio::test]
async fn multipart_completion_reclaims_reservation_when_authority_lookup_fails() {
    let fixture = Fixture::new(6);
    let authority = FaultStore::new();
    let pool = PoolObjectStore::initialize(
        authority.clone(),
        Path::from("db"),
        fixture.catalog.path().join("catalog"),
        fixture.members(),
    )
    .await
    .unwrap();
    let path = segment(1);
    let mut upload = multipart(&pool, &path, 6).await;
    upload
        .put_part(Bytes::from_static(b"abcdef").into())
        .await
        .unwrap();
    assert_eq!(usage(&pool, "hdd-a").await, (0, 6, 0));

    // Completion must recheck conditional-create visibility. Failure before
    // the physical PUT must still retire its already-owned reservation.
    authority.control.offline.store(true, Ordering::SeqCst);
    assert!(upload.complete().await.is_err());
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
    assert_eq!(pool.status().await.unwrap().pending_operations, 0);

    authority.control.offline.store(false, Ordering::SeqCst);
    assert!(matches!(
        pool.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    create(&pool, &path, b"abcdef").await;
    assert_eq!(usage(&pool, "hdd-a").await, (6, 0, 1));
}
