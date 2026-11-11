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
            health_device: None,
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
    stream_eio_payload_len: AtomicU64,
    stream_transient_payload_len: AtomicU64,
    gate_payload_len: AtomicU64,
    panic_payload_len: AtomicU64,
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
        if length != 0 && self.control.panic_payload_len.load(Ordering::SeqCst) == length {
            panic!("injected migration worker panic");
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
        let native = self.control.stream_eio_payload_len.load(Ordering::SeqCst) != 0
            && self.control.stream_eio_payload_len.load(Ordering::SeqCst) == result.meta.size;
        let transient = self
            .control
            .stream_transient_payload_len
            .load(Ordering::SeqCst)
            != 0
            && self
                .control
                .stream_transient_payload_len
                .load(Ordering::SeqCst)
                == result.meta.size;
        if !head && (native || transient) {
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            return Ok(GetResult {
                meta,
                range,
                attributes,
                extensions,
                payload: GetResultPayload::Stream(
                    futures::stream::once(async move {
                        Err(object_store::Error::Generic {
                            store: "PoolFaultStore",
                            source: if native {
                                Box::new(std::io::Error::from_raw_os_error(libc::EIO))
                            } else {
                                "transient streamed availability failure".into()
                            },
                        })
                    })
                    .boxed(),
                ),
            });
        }
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

#[tokio::test]
async fn disk_health_quarantine_fences_explicit_target_and_survives_restart() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(990);
    create(&pool, &path, b"source").await;
    pool.quarantine("hdd-b", "pending sectors").await.unwrap();
    assert!(pool.migrate(&path, "hdd-b").await.is_err());
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"source"));
    drop(pool);
    let pool = f.reopen().await.unwrap();
    assert!(
        pool.status().await.unwrap().members[1]
            .health
            .quarantine
            .is_some()
    );
    assert!(pool.migrate(&path, "hdd-b").await.is_err());
}

#[tokio::test]
async fn disk_health_quarantine_orders_against_started_put_publication() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    f.first.control.gate_payload_len.store(7, Ordering::SeqCst);
    let writer = pool.clone();
    let path = segment(991);
    let key = path.clone();
    let task = tokio::spawn(async move {
        writer
            .put_opts(
                &key,
                Bytes::from_static(b"blocked").into(),
                create_options(),
            )
            .await
    });
    f.first.control.entered.notified().await;
    pool.quarantine("hdd-a", "kernel I/O").await.unwrap();
    f.first.control.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(pool.head(&path).await.is_err());
    create(&pool, &segment(992), b"healthy").await;
    assert_eq!(placement(&pool, &segment(992)).await, "hdd-b");
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
}

#[tokio::test]
async fn disk_health_quarantine_fences_reserved_start_and_late_multipart() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(993);
    let upload = pool.reserve(&path, 4, Some("hdd-a".into())).await.unwrap();
    let mut multipart = multipart(&pool, &segment(994), 4).await;
    multipart
        .put_part(Bytes::from_static(b"data").into())
        .await
        .unwrap();
    pool.quarantine("hdd-a", "media fault").await.unwrap();
    let id = upload.id.clone();
    assert!(
        pool.catalog(move |c| c.mark_started_for_migration(&id, false))
            .await
            .is_err()
    );
    assert!(multipart.complete().await.is_err());
    assert!(pool.head(&segment(994)).await.is_err());
    pool.cleanup_upload(&upload).await.unwrap();
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
}

#[tokio::test]
async fn disk_health_evacuation_continues_after_unreadable_and_persists_loss() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let bad = segment(995);
    let good = segment(996);
    create(&pool, &bad, b"bad").await;
    create(&pool, &good, b"healthy").await;
    f.first
        .control
        .corrupt_payload_len
        .store(3, Ordering::SeqCst);
    pool.quarantine("hdd-a", "SMART pending sectors")
        .await
        .unwrap();
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(placement(&pool, &good).await, "hdd-b");
    assert_eq!(placement(&pool, &bad).await, "hdd-a");
    assert_eq!(read(&pool, &good).await, Bytes::from_static(b"healthy"));
    let status = pool.status().await.unwrap();
    assert_eq!(status.unreadable.len(), 1);
    assert_eq!(
        status.members[0].evacuation,
        "readable_evacuated_loss_blocked"
    );
    assert!(!status.members[0].safe_to_remove);
    assert!(status.members[0].safe_to_pull_with_known_loss);
    drop(pool);
    let reopened = f.reopen().await.unwrap();
    assert_eq!(reopened.faults().await.unwrap().len(), 1);
    assert!(!reopened.status().await.unwrap().members[0].safe_to_remove);
}

#[tokio::test]
async fn disk_health_evacuation_no_capacity_never_claims_completion() {
    let f = Fixture::new(8);
    let pool = f.initialize().await;
    create(&pool, &segment(997), b"12345678").await;
    create(&pool, &segment(998), b"abcdefgh").await;
    pool.quarantine("hdd-a", "kernel I/O").await.unwrap();
    let before = pool.status().await.unwrap();
    assert_eq!(before.members[0].evacuation, "cannot_complete_capacity");
    assert_eq!(before.members[0].evacuation_capacity.required_bytes, 8);
    assert_eq!(before.members[0].evacuation_capacity.available_bytes, 0);
    assert!(!before.members[0].evacuation_capacity.sufficient);
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].objects, 1);
    assert!(!status.members[0].safe_to_remove);
    assert!(!status.members[0].readable_evacuated);
    assert_eq!(status.members[0].evacuation, "cannot_complete_capacity");
    assert!(!status.members[0].safe_to_pull_with_known_loss);
    assert!(status.unreadable.is_empty());
}

#[tokio::test]
async fn physical_root_capacity_gate_observes_real_filesystem_reserve() {
    let root = tempfile::tempdir().unwrap();
    for name in ["authority", "hdd-a", "hdd-b"] {
        std::fs::create_dir(root.path().join(name)).unwrap();
    }
    let pool = persistent_pool(root.path(), true).await;
    let location = segment(1020);
    create(&pool, &location, b"one-copy").await;
    pool.quarantine("hdd-a", "media failure").await.unwrap();
    let authority = pool.inner.authority.clone();
    let mut members = pool.inner.members.clone();
    members[1].min_free_bytes = 1 << 60;
    drop(pool);
    let pool = PoolObjectStore::open(
        authority,
        Path::from("db"),
        root.path().join("catalog"),
        members,
    )
    .await
    .unwrap();
    let before = pool.status().await.unwrap();
    assert!(before.members[1].capacity_bytes > before.members[0].used_bytes);
    assert_eq!(before.members[1].available_bytes, 0);
    assert_eq!(before.members[0].evacuation, "cannot_complete_capacity");
    assert!(!before.members[0].evacuation_capacity.sufficient);
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(placement(&pool, &location).await, "hdd-a");
    assert_eq!(read(&pool, &location).await.as_ref(), b"one-copy");
    assert!(!pool.status().await.unwrap().members[0].safe_to_remove);
}

#[tokio::test]
async fn two_through_five_physical_roots_preserve_exact_unique_placements() {
    for count in 2..=5 {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("authority")).unwrap();
        let authority: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(root.path().join("authority"))
                .unwrap(),
        );
        let mut members = Vec::new();
        for index in 0..count {
            let id = format!("disk-{index}");
            let directory = root.path().join(&id);
            std::fs::create_dir(&directory).unwrap();
            members.push(PoolMember {
                identity: format!("file:///{id}"),
                id,
                store: Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(&directory).unwrap(),
                ),
                capacity_bytes: 8,
                local_path: Some(directory.canonicalize().unwrap()),
                min_free_bytes: 0,
                health_device: None,
            });
        }
        let pool = PoolObjectStore::initialize(
            authority.clone(),
            Path::from("db"),
            root.path().join("catalog"),
            members.clone(),
        )
        .await
        .unwrap();
        for index in 0..count {
            pool.put_opts(
                &segment(2000 + index),
                Bytes::from(vec![index as u8; 8]).into(),
                create_options(),
            )
            .await
            .unwrap();
        }
        let placements = pool.placements().await.unwrap();
        assert_eq!(placements.len(), count as usize);
        let before = serde_json::to_value(&placements).unwrap();
        for (index, entry) in placements.iter().enumerate() {
            assert_eq!(entry.member_id, format!("disk-{index}"));
            let body = root.path().join(&entry.member_id).join(&entry.physical_key);
            assert_eq!(std::fs::read(body).unwrap(), vec![index as u8; 8]);
            for other in 0..count {
                if entry.member_id != format!("disk-{other}") {
                    assert!(
                        !root
                            .path()
                            .join(format!("disk-{other}"))
                            .join(&entry.physical_key)
                            .exists()
                    );
                }
            }
        }
        drop(pool);
        let reopened = PoolObjectStore::open(
            authority,
            Path::from("db"),
            root.path().join("catalog"),
            members,
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(reopened.placements().await.unwrap()).unwrap(),
            before
        );
        for index in 0..count {
            assert_eq!(
                read(&reopened, &segment(2000 + index)).await.as_ref(),
                &vec![index as u8; 8]
            );
        }
    }
}

#[tokio::test]
async fn disk_health_destination_corruption_does_not_label_source_unreadable() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(999);
    create(&pool, &path, b"source").await;
    f.second
        .control
        .corrupt_payload_len
        .store(6, Ordering::SeqCst);
    pool.quarantine("hdd-a", "kernel I/O").await.unwrap();
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(placement(&pool, &path).await, "hdd-a");
    assert!(pool.faults().await.unwrap().is_empty());
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"source"));
    assert!(!pool.status().await.unwrap().members[0].safe_to_remove);
}

#[tokio::test]
async fn disk_health_streamed_eio_is_durable_and_does_not_stop_other_copies() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let bad = segment(1000);
    let good = segment(1001);
    create(&pool, &bad, b"damaged").await;
    create(&pool, &good, b"good").await;
    f.first
        .control
        .stream_eio_payload_len
        .store(7, Ordering::SeqCst);
    assert!(pool.get(&bad).await.unwrap().bytes().await.is_err());
    assert!(
        pool.status().await.unwrap().members[0]
            .health
            .quarantine
            .is_some()
    );
    assert_eq!(pool.faults().await.unwrap().len(), 1);
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(placement(&pool, &good).await, "hdd-b");
    let no_space = object_store::Error::Generic {
        store: "fixture",
        source: Box::new(std::io::Error::from_raw_os_error(libc::ENOSPC)),
    };
    pool.observe_io_error("hdd-b", &no_space).await.unwrap();
    assert!(
        pool.status().await.unwrap().members[1]
            .health
            .quarantine
            .is_none()
    );
}

#[tokio::test]
async fn disk_health_settlement_waits_for_cancelled_inner_put_cleanup() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    f.first.control.gate_payload_len.store(4, Ordering::SeqCst);
    let writer = pool.clone();
    let task = tokio::spawn(async move {
        writer
            .put_opts(
                &segment(1002),
                Bytes::from_static(b"data").into(),
                create_options(),
            )
            .await
    });
    f.first.control.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let draining = pool.clone();
    let settlement = tokio::spawn(async move { draining.settle().await });
    tokio::task::yield_now().await;
    assert!(!settlement.is_finished());
    f.first.control.release.notify_one();
    settlement.await.unwrap();
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
    assert!(pool.head(&segment(1002)).await.is_err());
}

#[tokio::test]
async fn disk_health_transient_get_stream_and_invalid_range_do_not_poison_data() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(1003);
    create(&pool, &path, b"healthy").await;
    f.first.control.offline.store(true, Ordering::SeqCst);
    assert!(pool.get(&path).await.is_err());
    f.first.control.offline.store(false, Ordering::SeqCst);
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"healthy"));
    f.first
        .control
        .stream_transient_payload_len
        .store(7, Ordering::SeqCst);
    assert!(pool.get(&path).await.unwrap().bytes().await.is_err());
    f.first
        .control
        .stream_transient_payload_len
        .store(0, Ordering::SeqCst);
    assert!(
        pool.get_opts(
            &path,
            GetOptions {
                range: Some(GetRange::Bounded(99..100)),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"healthy"));
    assert!(pool.faults().await.unwrap().is_empty());
    drop(pool);
    let pool = f.reopen().await.unwrap();
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"healthy"));
    assert!(pool.faults().await.unwrap().is_empty());
}

#[tokio::test]
async fn disk_health_debt_retry_cleans_quarantined_source_and_preserves_live_upload() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    create(&pool, &segment(1004), b"source").await;
    f.first.control.fail_deletes.store(true, Ordering::SeqCst);
    pool.quarantine("hdd-a", "SMART media failure")
        .await
        .unwrap();
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    let before = pool.status().await.unwrap();
    assert_eq!(before.members[0].objects, 0);
    assert_eq!(before.members[0].used_bytes, 6);
    assert!(!before.members[0].safe_to_remove);
    let mut active = multipart(&pool, &segment(1005), 4).await;
    pool.recover_pending().await.unwrap();
    assert_eq!(usage(&pool, "hdd-b").await.1, 4);
    f.first.control.fail_deletes.store(false, Ordering::SeqCst);
    pool.recover_pending().await.unwrap();
    active.abort().await.unwrap();
    drop(active);
    let after = pool.status().await.unwrap();
    assert_eq!(after.members[0].used_bytes, 0);
    assert_eq!(after.members[0].reserved_bytes, 0);
    assert_eq!(after.pending_operations, 0);
    assert!(after.members[0].safe_to_remove);
    let prefix = Path::from(format!(".zerofs-pool/{}/objects", after.pool_id));
    assert!(
        f.first
            .inner
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn disk_health_verified_same_generation_retry_clears_source_fault() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(1006);
    create(&pool, &path, b"source").await;
    f.first
        .control
        .corrupt_payload_len
        .store(6, Ordering::SeqCst);
    assert!(pool.migrate(&path, "hdd-b").await.is_err());
    let fault = pool.faults().await.unwrap().pop().unwrap();
    f.first
        .control
        .corrupt_payload_len
        .store(0, Ordering::SeqCst);
    pool.verify_source_fault(&fault).await.unwrap();
    assert!(pool.faults().await.unwrap().is_empty());
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"source"));
    assert!(
        pool.status().await.unwrap().members[0]
            .health
            .quarantine
            .is_none()
    );
}

#[tokio::test]
async fn disk_health_partial_smart_failure_quarantines_and_evacuates() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    create(&pool, &segment(1007), b"source").await;
    pool.observe_health(
        "hdd-a",
        super::SmartSample {
            device: "disk".into(),
            pending: Some(1),
            ..Default::default()
        },
        Some("CRC telemetry unavailable".into()),
    )
    .await
    .unwrap();
    pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(placement(&pool, &segment(1007)).await, "hdd-b");
    let status = pool.status().await.unwrap();
    assert!(status.members[0].health.quarantine.is_some());
    assert!(status.members[0].health.monitoring_error.is_some());
    assert!(status.members[0].safe_to_remove);
}

async fn persistent_pool(root: &std::path::Path, initialize: bool) -> PoolObjectStore {
    let root = root.canonicalize().unwrap();
    let local = |name: &str| -> Arc<dyn ObjectStore> {
        Arc::new(object_store::prefix::PrefixStore::new(
            crate::local_object_store::LocalConditionalStore::new(),
            Path::from_filesystem_path(root.join(name)).unwrap(),
        ))
    };
    let members = ["hdd-a", "hdd-b"]
        .into_iter()
        .map(|id| PoolMember {
            id: id.into(),
            identity: format!("file://{id}"),
            store: local(id),
            capacity_bytes: 1024 * 1024,
            local_path: Some(root.join(id)),
            min_free_bytes: 0,
            health_device: None,
        })
        .collect();
    if initialize {
        PoolObjectStore::initialize(
            local("authority"),
            Path::from("db"),
            root.join("catalog"),
            members,
        )
        .await
        .unwrap()
    } else {
        PoolObjectStore::open(
            local("authority"),
            Path::from("db"),
            root.join("catalog"),
            members,
        )
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn native_staging_recovery_reclaims_owned_files_before_releasing_reservation() {
    for final_exists in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        for directory in ["authority", "hdd-a", "hdd-b"] {
            std::fs::create_dir(temp.path().join(directory)).unwrap();
        }
        let pool = persistent_pool(temp.path(), true).await;
        let source = segment(2100);
        create(&pool, &source, b"original").await;
        let upload = pool
            .reserve(&source, 8, Some("hdd-b".into()))
            .await
            .unwrap();
        let id = upload.id.clone();
        pool.catalog(move |catalog| catalog.mark_started_for_migration(&id, false))
            .await
            .unwrap();
        pool.ensure_member("hdd-b", true).await.unwrap();
        let physical = temp.path().join("hdd-b").join(&upload.physical_key);
        std::fs::create_dir_all(physical.parent().unwrap()).unwrap();
        let staging: Vec<_> = [1, 7]
            .into_iter()
            .map(|suffix| {
                let mut path = physical.as_os_str().to_owned();
                path.push(format!("#{suffix}"));
                std::path::PathBuf::from(path)
            })
            .collect();
        for path in &staging {
            std::fs::write(path, "partial").unwrap();
        }
        if final_exists {
            std::fs::hard_link(&staging[0], &physical).unwrap();
        }
        let unrelated = physical
            .parent()
            .unwrap()
            .join(format!("{}#1", uuid::Uuid::new_v4()));
        std::fs::write(&unrelated, "unrelated").unwrap();
        pool.quarantine("hdd-b", "interrupted destination")
            .await
            .unwrap();
        let before = pool.status().await.unwrap();
        assert_eq!(before.members[1].reserved_bytes, 8);
        assert!(!before.members[1].safe_to_remove);
        drop(upload);
        drop(pool);

        let reopened = persistent_pool(temp.path(), false).await;
        assert_eq!(read(&reopened, &source).await, "original");
        assert_eq!(placement(&reopened, &source).await, "hdd-a");
        assert!(!physical.exists());
        assert!(staging.iter().all(|path| !path.exists()));
        assert_eq!(std::fs::read(unrelated).unwrap(), b"unrelated");
        let status = reopened.status().await.unwrap();
        assert_eq!(status.pending_operations, 0);
        assert_eq!(status.members[1].reserved_bytes, 0);
        assert!(status.members[1].safe_to_remove);
        reopened.recover_pending().await.unwrap();
        assert_eq!(read(&reopened, &source).await, "original");
    }
}

#[tokio::test]
async fn native_staging_cleanup_failure_keeps_capacity_charged_and_pull_blocked() {
    let temp = tempfile::tempdir().unwrap();
    for directory in ["authority", "hdd-a", "hdd-b"] {
        std::fs::create_dir(temp.path().join(directory)).unwrap();
    }
    let pool = persistent_pool(temp.path(), true).await;
    let source = segment(2101);
    create(&pool, &source, b"original").await;
    let upload = pool
        .reserve(&source, 8, Some("hdd-b".into()))
        .await
        .unwrap();
    let id = upload.id.clone();
    pool.catalog(move |catalog| catalog.mark_started_for_migration(&id, false))
        .await
        .unwrap();
    pool.ensure_member("hdd-b", true).await.unwrap();
    let staged = temp
        .path()
        .join("hdd-b")
        .join(format!("{}#1", upload.physical_key));
    std::fs::create_dir_all(&staged).unwrap();
    pool.quarantine("hdd-b", "interrupted destination")
        .await
        .unwrap();
    drop(upload);
    drop(pool);

    let reopened = persistent_pool(temp.path(), false).await;
    let status = reopened.status().await.unwrap();
    assert_eq!(status.pending_operations, 1);
    assert_eq!(status.members[1].reserved_bytes, 8);
    assert_eq!(status.members[1].pending_uploads, 1);
    assert!(!status.members[1].safe_to_remove);
    assert!(!status.members[1].safe_to_pull_with_known_loss);
    assert!(
        staged.is_dir(),
        "cleanup must not recursively delete unknown contents"
    );
    assert_eq!(read(&reopened, &source).await, "original");
    assert!(!reopened.inner.safety_failure.is_cancelled());

    std::fs::remove_dir(staged).unwrap();
    reopened.recover_pending().await.unwrap();
    let status = reopened.status().await.unwrap();
    assert_eq!(status.pending_operations, 0);
    assert_eq!(status.members[1].reserved_bytes, 0);
    assert!(status.members[1].safe_to_remove);
}

#[tokio::test]
async fn native_recovery_timeout_keeps_cleanup_owned_until_catalog_settlement() {
    let temp = tempfile::tempdir().unwrap();
    for directory in ["authority", "hdd-a", "hdd-b"] {
        std::fs::create_dir(temp.path().join(directory)).unwrap();
    }
    let pool = persistent_pool(temp.path(), true).await;
    let source = segment(2102);
    create(&pool, &source, b"original").await;
    let upload = pool
        .reserve(&source, 8, Some("hdd-b".into()))
        .await
        .unwrap();
    let id = upload.id.clone();
    let mark = id.clone();
    pool.catalog(move |catalog| catalog.mark_started_for_migration(&mark, false))
        .await
        .unwrap();
    pool.ensure_member("hdd-b", true).await.unwrap();
    let staged = temp
        .path()
        .join("hdd-b")
        .join(format!("{}#1", upload.physical_key));
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    std::fs::write(&staged, "partial").unwrap();
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(staged.parent().unwrap().join(".zerofs-local-mutations#0"))
        .unwrap();
    fs4::fs_std::FileExt::lock_exclusive(&lock).unwrap();
    pool.quarantine("hdd-b", "interrupted destination")
        .await
        .unwrap();
    drop(upload);

    // Keep the native lock held through the recovery attempt's one-second
    // deadline. Its wait must return while the tracked cleanup retains both
    // active-upload ownership and the logical key write guard.
    tokio::time::timeout(Duration::from_secs(5), pool.recover_pending())
        .await
        .unwrap()
        .unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(status.pending_operations, 1);
    assert_eq!(status.members[1].reserved_bytes, 8);
    assert!(!status.members[1].safe_to_remove);
    assert!(staged.exists());
    assert!(pool.inner.active_uploads.lock().unwrap().contains(&id));
    assert!(pool.key_lock(&source).try_write_owned().is_err());

    drop(lock);
    // No second recovery pass: the same owned task must delete the stage and
    // durably retire its reservation after the underlying I/O is unblocked.
    tokio::time::timeout(Duration::from_secs(5), pool.settle())
        .await
        .unwrap();
    assert!(!staged.exists());
    assert!(!pool.inner.active_uploads.lock().unwrap().contains(&id));
    assert!(pool.key_lock(&source).try_write_owned().is_ok());
    let status = pool.status().await.unwrap();
    assert_eq!(status.pending_operations, 0);
    assert_eq!(status.members[1].reserved_bytes, 0);
    assert!(status.members[1].safe_to_remove);
    assert_eq!(read(&pool, &source).await, "original");
    assert!(!pool.inner.safety_failure.is_cancelled());
}

#[tokio::test]
#[ignore = "child of disk_health_crash_reopen_preserves_one_verified_placement"]
async fn disk_health_crash_child() {
    let Some(root) = std::env::var_os("ZEROFS_POOL_CRASH_ROOT") else {
        return;
    };
    let pool = persistent_pool(std::path::Path::new(&root), true).await;
    create(&pool, &segment(2000), b"crash-safe source").await;
    pool.quarantine("hdd-a", "fixture media failure")
        .await
        .unwrap();
    super::CRASH_ARMED.store(true, Ordering::SeqCst);
    pool.migrate(&segment(2000), "hdd-b").await.unwrap();
    panic!("requested crash cutoff was not reached");
}

#[tokio::test]
async fn disk_health_crash_reopen_preserves_one_verified_placement() {
    for cutoff in [
        "after_reserve",
        "after_started",
        "after_target_write",
        "after_verification",
        "after_remap",
        "before_source_delete",
        "after_source_delete",
    ] {
        let temp = tempfile::tempdir().unwrap();
        for directory in ["authority", "hdd-a", "hdd-b"] {
            std::fs::create_dir(temp.path().join(directory)).unwrap();
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "pool_object_store::tests::disk_health_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("ZEROFS_POOL_CRASH_ROOT", temp.path())
            .env("ZEROFS_POOL_CRASH_CUT", cutoff)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("crash child timed out at {cutoff}");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(status.code(), Some(86), "cutoff {cutoff}");
        let pool = persistent_pool(temp.path(), false).await;
        assert_eq!(
            read(&pool, &segment(2000)).await,
            Bytes::from_static(b"crash-safe source")
        );
        let status = pool.status().await.unwrap();
        assert_eq!(status.mapped_objects, 1, "cutoff {cutoff}");
        assert_eq!(status.pending_operations, 0, "cutoff {cutoff}");
        assert!(status.members[0].health.quarantine.is_some());
        let committed = matches!(
            cutoff,
            "after_remap" | "before_source_delete" | "after_source_delete"
        );
        assert_eq!(
            placement(&pool, &segment(2000)).await,
            if committed { "hdd-b" } else { "hdd-a" }
        );
        let mut bodies = 0;
        for member in ["hdd-a", "hdd-b"] {
            let objects = temp
                .path()
                .join(member)
                .join(".zerofs-pool")
                .join(&status.pool_id)
                .join("objects");
            if objects.exists() {
                bodies += std::fs::read_dir(objects)
                    .unwrap()
                    .filter(|entry| {
                        entry.as_ref().unwrap().file_name() != ".zerofs-local-mutations#0"
                    })
                    .count();
            }
        }
        assert_eq!(bodies, 1, "uncharged or lost physical body at {cutoff}");
    }
}

#[tokio::test]
async fn disk_health_settlement_holds_returned_read_and_multipart_leases() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    create(&pool, &segment(2001), b"source").await;
    let read = pool.get(&segment(2001)).await.unwrap();
    let mut upload = multipart(&pool, &segment(2002), 4).await;
    let draining = pool.clone();
    let settlement = tokio::spawn(async move { draining.settle().await });
    tokio::task::yield_now().await;
    assert!(!settlement.is_finished());
    drop(read);
    upload.abort().await.unwrap();
    assert!(!settlement.is_finished());
    drop(upload);
    tokio::time::timeout(Duration::from_secs(2), settlement)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pool.status().await.unwrap().pending_operations, 0);
}

#[tokio::test]
async fn disk_health_crc_history_survives_partial_telemetry_and_restart() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    for crc in [Some(0), Some(1), Some(1), None] {
        pool.observe_health(
            "hdd-a",
            super::SmartSample {
                device: "disk".into(),
                crc,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    }
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].health.crc_increases, 1);
    assert!(status.members[0].health.quarantine.is_none());
    drop(pool);
    let pool = f.reopen().await.unwrap();
    pool.observe_health(
        "hdd-a",
        super::SmartSample {
            device: "disk".into(),
            crc: Some(2),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].health.crc_increases, 2);
    assert!(status.members[0].health.quarantine.is_some());
}

#[tokio::test]
async fn disk_health_empty_missing_object_cannot_clear_without_physical_verification() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(2003);
    create(&pool, &path, b"").await;
    let entry = pool.visible(&path).await.unwrap().unwrap();
    f.first
        .inner
        .delete(&Path::from(entry.physical.key()))
        .await
        .unwrap();
    assert!(pool.get(&path).await.is_err());
    let fault = pool.faults().await.unwrap().pop().unwrap();
    pool.verify_source_fault(&fault).await.unwrap();
    assert_eq!(pool.faults().await.unwrap().len(), 1);
}

#[tokio::test]
async fn disk_health_worker_panic_during_copy_signals_failure_and_retains_debt() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(2004);
    create(&pool, &path, b"source").await;
    pool.quarantine("hdd-a", "fixture media failure")
        .await
        .unwrap();
    f.second.control.gate_payload_len.store(6, Ordering::SeqCst);
    f.second
        .control
        .panic_payload_len
        .store(6, Ordering::SeqCst);
    let failure = tokio_util::sync::CancellationToken::new();
    let worker = super::PoolRecovery::start(pool.clone(), failure.clone());
    f.second.control.entered.notified().await;
    f.second.control.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), failure.cancelled())
        .await
        .unwrap();
    assert!(worker.shutdown().await.is_err());
    pool.settle().await;
    assert_eq!(placement(&pool, &path).await, "hdd-a");
    assert_eq!(usage(&pool, "hdd-b").await.1, 6);
    pool.recover_pending().await.unwrap();
    assert_eq!(usage(&pool, "hdd-b").await.1, 0);
    assert_eq!(read(&pool, &path).await, Bytes::from_static(b"source"));
}

#[tokio::test]
async fn disk_health_second_disk_observation_progresses_during_blocked_copy() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(2005);
    create(&pool, &path, b"source").await;
    pool.quarantine("hdd-a", "first failed disk").await.unwrap();
    f.second.control.gate_payload_len.store(6, Ordering::SeqCst);
    let worker =
        super::PoolRecovery::start(pool.clone(), tokio_util::sync::CancellationToken::new());
    f.second.control.entered.notified().await;
    tokio::time::timeout(
        Duration::from_secs(1),
        pool.observe_health(
            "hdd-b",
            super::SmartSample {
                device: "second".into(),
                pending: Some(1),
                ..Default::default()
            },
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        pool.status().await.unwrap().members[1]
            .health
            .quarantine
            .is_some()
    );
    f.second.control.release.notify_one();
    worker.shutdown().await.unwrap();
    pool.settle().await;
    let status = pool.status().await.unwrap();
    assert_eq!(placement(&pool, &path).await, "hdd-a");
    assert!(!status.members[0].safe_to_remove);
    assert!(!status.members[0].readable_evacuated);
    assert_eq!(status.members[1].objects, 0);
    assert_eq!(status.pending_operations, 0);
    assert!(status.unreadable.is_empty());
}

#[tokio::test]
async fn disk_health_catalog_publication_is_fenced_after_upload_start() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(2006);
    let upload = pool.reserve(&path, 4, Some("hdd-a".into())).await.unwrap();
    let id = upload.id.clone();
    pool.catalog(move |c| c.mark_started_for_migration(&id, false))
        .await
        .unwrap();
    pool.quarantine("hdd-a", "media failure after start")
        .await
        .unwrap();
    let entry = super::Entry {
        physical: super::Physical::Member {
            member_id: upload.member_id.clone(),
            key: upload.physical_key.clone(),
            e_tag: None,
            version: None,
        },
        size: 4,
        last_modified_secs: 0,
        last_modified_nanos: 0,
        e_tag: Some("logical".into()),
        version: None,
        sha256: None,
        attributes: Vec::new(),
    };
    let id = upload.id.clone();
    assert!(
        pool.catalog(move |c| c.publish_for_migration(&id, &entry, None, false))
            .await
            .is_err()
    );
    assert!(pool.visible(&path).await.unwrap().is_none());
    assert_eq!(usage(&pool, "hdd-a").await, (0, 4, 0));
    pool.cleanup_upload(&upload).await.unwrap();
    assert_eq!(usage(&pool, "hdd-a").await, (0, 0, 0));
}

#[tokio::test]
async fn disk_health_kernel_cursor_deduplicates_replay_and_handles_reboot_and_gaps() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    pool.kernel_observation("hdd-a", "available".into(), Some(("boot-a".into(), 42)))
        .await
        .unwrap();
    let since = pool.status().await.unwrap().members[0].health.since_ms;
    pool.kernel_observation(
        "hdd-a",
        "ring gap; coverage incomplete".into(),
        Some(("boot-a".into(), 41)),
    )
    .await
    .unwrap();
    assert_eq!(
        pool.status().await.unwrap().members[0].health.kernel_cursor,
        Some(("boot-a".into(), 42))
    );
    drop(pool);
    let pool = f.reopen().await.unwrap();
    pool.kernel_observation("hdd-a", "new boot".into(), Some(("boot-b".into(), 1)))
        .await
        .unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(
        status.members[0].health.kernel_cursor,
        Some(("boot-b".into(), 1))
    );
    assert_eq!(status.members[0].health.since_ms, since);
    pool.kernel_observation(
        "hdd-b",
        "permission denied; coverage unavailable".into(),
        None,
    )
    .await
    .unwrap();
    let status = pool.status().await.unwrap();
    assert!(status.members[1].health.quarantine.is_none());
    assert!(
        status.members[1]
            .health
            .kernel_coverage
            .as_ref()
            .unwrap()
            .contains("permission denied")
    );
}

#[tokio::test]
async fn disk_health_zero_byte_debts_prevent_safe_removal_until_cleaned() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let upload = pool
        .reserve(&segment(2007), 0, Some("hdd-a".into()))
        .await
        .unwrap();
    let id = upload.id.clone();
    pool.catalog(move |c| c.mark_started_for_migration(&id, false))
        .await
        .unwrap();
    f.first
        .inner
        .put(
            &Path::from(upload.physical_key.as_str()),
            Bytes::new().into(),
        )
        .await
        .unwrap();
    pool.quarantine("hdd-a", "media failure").await.unwrap();
    f.first.control.fail_deletes.store(true, Ordering::SeqCst);
    assert!(pool.cleanup_upload(&upload).await.is_err());
    drop(upload);
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].reserved_bytes, 0);
    assert_eq!(status.members[0].pending_uploads, 1);
    assert!(!status.members[0].safe_to_remove);
    f.first.control.fail_deletes.store(false, Ordering::SeqCst);
    pool.recover_pending().await.unwrap();
    assert!(pool.status().await.unwrap().members[0].safe_to_remove);

    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let path = segment(2008);
    create(&pool, &path, b"").await;
    pool.quarantine("hdd-a", "media failure").await.unwrap();
    f.first.control.fail_deletes.store(true, Ordering::SeqCst);
    pool.migrate(&path, "hdd-b").await.unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].used_bytes, 0);
    assert_eq!(status.members[0].cleanup_debts, 1);
    assert!(!status.members[0].safe_to_remove);
    f.first.control.fail_deletes.store(false, Ordering::SeqCst);
    pool.recover_pending().await.unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].cleanup_debts, 0);
    assert!(status.members[0].safe_to_remove);
}

#[tokio::test]
async fn disk_health_missing_monitored_device_does_not_block_healthy_members_on_startup() {
    let temp = tempfile::tempdir().unwrap();
    for directory in ["authority", "hdd-a", "hdd-b", "cache"] {
        std::fs::create_dir(temp.path().join(directory)).unwrap();
    }
    let pool = persistent_pool(temp.path(), true).await;
    create(&pool, &segment(2009), b"source").await;
    let config = format!(
        r#"
[storage]
url = {:?}
encryption_password = "fixture-password"
[storage.pool]
catalog_dir = {:?}
[[storage.pool.members]]
id = "hdd-a"
url = {:?}
capacity_gb = 0.001
reserve_free_gb = 0.0
health_device = {:?}
[[storage.pool.members]]
id = "hdd-b"
url = {:?}
capacity_gb = 0.001
reserve_free_gb = 0.0
[cache]
dir = {:?}
disk_size_gb = 0.1
memory_size_gb = 0.064
[servers.ninep]
unix_socket = {:?}
[telemetry]
enabled = false
"#,
        url::Url::from_directory_path(temp.path().join("authority"))
            .unwrap()
            .as_str(),
        temp.path().join("catalog"),
        url::Url::from_directory_path(temp.path().join("hdd-a"))
            .unwrap()
            .as_str(),
        temp.path().join("missing-device"),
        url::Url::from_directory_path(temp.path().join("hdd-b"))
            .unwrap()
            .as_str(),
        temp.path().join("cache"),
        temp.path().join("ninep.sock")
    );
    let config_path = temp.path().join("settings.toml");
    std::fs::write(&config_path, config).unwrap();
    let settings = crate::config::Settings::from_file(&config_path).unwrap();
    let prepared = crate::disk_health::PreparedDiskMonitor::prepare(pool.clone(), &settings)
        .await
        .unwrap();
    let status = pool.status().await.unwrap();
    assert!(status.members[0].health.quarantine.is_some());
    assert!(status.members[0].health.monitoring_error.is_some());
    create(&pool, &segment(2010), b"healthy").await;
    assert_eq!(placement(&pool, &segment(2010)).await, "hdd-b");
    drop(prepared);
    drop(pool);
    let pool = persistent_pool(temp.path(), false).await;
    let _prepared = crate::disk_health::PreparedDiskMonitor::prepare(pool.clone(), &settings)
        .await
        .unwrap();
    assert_eq!(
        read(&pool, &segment(2010)).await,
        Bytes::from_static(b"healthy")
    );
    assert!(
        pool.status().await.unwrap().members[0]
            .health
            .quarantine
            .is_some()
    );
}

#[tokio::test]
async fn disk_health_transient_evacuation_never_authorizes_known_loss() {
    for streamed in [false, true] {
        let f = Fixture::new(1024);
        let pool = f.initialize().await;
        let path = segment(1200);
        create(&pool, &path, b"healthy").await;
        pool.quarantine("hdd-a", "SMART media failure")
            .await
            .unwrap();
        if streamed {
            f.first
                .control
                .stream_transient_payload_len
                .store(7, Ordering::SeqCst);
        } else {
            f.first.control.offline.store(true, Ordering::SeqCst);
        }
        pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
            .await
            .unwrap();
        let status = pool.status().await.unwrap();
        assert!(status.unreadable.is_empty());
        assert!(!status.members[0].safe_to_pull_with_known_loss);
        assert!(!status.members[0].readable_evacuated);
        f.first.control.offline.store(false, Ordering::SeqCst);
        f.first
            .control
            .stream_transient_payload_len
            .store(0, Ordering::SeqCst);
        assert_eq!(read(&pool, &path).await, Bytes::from_static(b"healthy"));
        pool.evacuate_pass(&tokio_util::sync::CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(placement(&pool, &path).await, "hdd-b");
        assert_eq!(read(&pool, &path).await, Bytes::from_static(b"healthy"));
        assert!(pool.status().await.unwrap().members[0].safe_to_remove);
    }
}

#[tokio::test]
async fn disk_health_debt_retry_rechecks_owner_after_key_lock() {
    let f = Fixture::new(1024);
    let pool = f.initialize().await;
    let reserved = Arc::new(Notify::new());
    let register = Arc::new(Notify::new());
    let checked = Arc::new(Notify::new());
    let recover = Arc::new(Notify::new());
    *pool.inner.reserve_pause.lock().unwrap() = Some((reserved.clone(), register.clone()));
    *pool.inner.recovery_pause.lock().unwrap() = Some((checked.clone(), recover.clone()));
    let writer = pool.clone();
    let creating = tokio::spawn(async move { multipart(&writer, &segment(1201), 4).await });
    reserved.notified().await;
    let cleaner = pool.clone();
    let cleaning = tokio::spawn(async move { cleaner.recover_pending().await });
    checked.notified().await;
    register.notify_one();
    let mut upload = creating.await.unwrap();
    recover.notify_one();
    cleaning.await.unwrap().unwrap();
    assert_eq!(usage(&pool, "hdd-a").await.1, 4);
    upload
        .put_part(Bytes::from_static(b"data").into())
        .await
        .unwrap();
    upload.complete().await.unwrap();
    drop(upload);
    assert_eq!(
        read(&pool, &segment(1201)).await,
        Bytes::from_static(b"data")
    );
    assert_eq!(usage(&pool, "hdd-a").await.1, 0);
    assert!(!pool.inner.safety_failure.is_cancelled());
}
