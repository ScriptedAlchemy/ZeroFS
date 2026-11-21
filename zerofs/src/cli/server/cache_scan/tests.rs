//! The scan report over cache directories the production builders wrote.

use super::super::{
    BLOCK_CACHE_DIR, FoyerMetricsRegistry, LEGACY_BLOCK_CACHE_DIR, PARTS_BLOCK_SIZE,
    acquire_clean_cache_owner, build_block_hybrid, build_parts_hybrid,
};
use super::{
    CacheKind, CacheLayout, CacheReport, ScanOptions, ScanReport, ScanStatus, blobs,
    scan_cache_files,
};
use crate::object_store_prefetch::{PARTS_CACHE_DIR, PARTS_CACHE_LAYOUT_FILE};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MIB: usize = 1024 * 1024;
/// A clean-cache namespace inside the `[cache] dir`, as the server names it.
const NAMESPACE: &str = "bucket_scan";

fn handle() -> tokio::runtime::Handle {
    tokio::runtime::Handle::current()
}

/// Fill a raw-parts cache through the production prefetching store.
async fn write_parts_cache(namespace: &Path, objects: u8) {
    let _owner = acquire_clean_cache_owner(namespace).unwrap();
    let parts = build_parts_hybrid(
        namespace,
        MIB,
        256 * MIB,
        &handle(),
        FoyerMetricsRegistry::default(),
    )
    .await
    .unwrap();
    let backend = Arc::new(object_store::memory::InMemory::new());
    let store =
        crate::object_store_prefetch::PrefetchingObjectStore::new(backend.clone(), parts.clone());
    for object in 0..objects {
        let path = ObjectPath::from(format!("segments/scan/{object}"));
        let bytes = Bytes::from(vec![object + 1; MIB + 1_000 * usize::from(object) + 777]);
        backend
            .put(&path, PutPayload::from_bytes(bytes.clone()))
            .await
            .unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            bytes
        );
    }
    parts.close().await.unwrap();
}

/// Fill a decoded-block cache through SlateDB reads over encrypted SSTs.
async fn write_block_cache(namespace: &Path, rows: u32) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let transformer = crate::block_transformer::ZeroFsBlockTransformer::new_arc(
        &[7; 32],
        crate::config::CompressionConfig::default(),
    );
    let builder = || {
        slatedb::DbBuilder::new(ObjectPath::from("db"), store.clone())
            .with_settings(slatedb::config::Settings {
                compactor_options: None,
                ..Default::default()
            })
            .with_sst_block_size(slatedb::SstBlockSize::Block32Kib)
            .with_block_transformer(transformer.clone())
    };
    let row = |key: u32| vec![(key % 251) as u8 + 1; 64 + (key % 300) as usize];
    let db = builder().build().await.unwrap();
    for key in 0..rows {
        db.put_with_options(
            key.to_be_bytes(),
            row(key),
            &slatedb::config::PutOptions::default(),
            &slatedb::config::WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let (cache, hybrid) = build_block_hybrid(
        &namespace.join(BLOCK_CACHE_DIR),
        MIB,
        512 * MIB,
        &handle(),
        FoyerMetricsRegistry::default(),
    )
    .await
    .unwrap();
    let db = builder().with_db_cache(cache).build().await.unwrap();
    for key in 0..rows {
        assert_eq!(
            db.get(key.to_be_bytes()).await.unwrap().unwrap().as_ref(),
            row(key).as_slice()
        );
    }
    db.close().await.unwrap();
    hybrid.close().await.unwrap();
}

/// A `[cache] dir` with one namespace holding both caches as the server
/// writes them.
async fn production_cache_dir() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let namespace = root.path().join(NAMESPACE);
    write_parts_cache(&namespace, 4).await;
    write_block_cache(&namespace, 4_000).await;
    (root, namespace)
}

fn cache(report: &ScanReport, kind: CacheKind) -> &CacheReport {
    let found: Vec<_> = report.caches.iter().filter(|c| c.kind == kind).collect();
    assert_eq!(found.len(), 1, "{kind:?} in {report:#?}");
    found[0]
}

fn scan(path: &Path) -> ScanReport {
    scan_cache_files(path, ScanOptions::default()).unwrap()
}

/// The first partition file of `dir` that holds a blob, its bytes and blobs.
fn first_written_block(dir: &Path) -> (PathBuf, Vec<u8>, Vec<super::BlobLayout>) {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    for path in paths {
        let block = std::fs::read(&path).unwrap();
        let found = blobs(&block);
        if !found.is_empty() {
            return (path, block, found);
        }
    }
    panic!("no written block in {}", dir.display());
}

fn plant_byte(path: &Path, offset: usize) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&[0x5a], offset as u64).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caches_the_server_writes_scan_clean() {
    let (root, namespace) = production_cache_dir().await;

    // The [cache] dir, one namespace, and one cache directory all resolve.
    for path in [root.path().to_path_buf(), namespace.clone()] {
        let report = scan(&path);
        assert_eq!(report.status, ScanStatus::Clean, "{report:#?}");
        assert_eq!(report.status.exit_code(), 0);
        for kind in [CacheKind::RawParts, CacheKind::DecodedBlocks] {
            let found = cache(&report, kind);
            assert_eq!(found.layout, CacheLayout::Current);
            let counts = found.scanned.as_ref().unwrap();
            assert!(counts.entries > 0, "{kind:?}: {counts:?}");
            assert!(counts.padding_bytes > 0 && counts.index_tail_bytes > 0);
            assert_eq!(counts.nonzero(), 0);
        }
    }
    let one = scan(&namespace.join(PARTS_CACHE_DIR));
    assert_eq!(one.caches.len(), 1);
    assert_eq!(one.caches[0].kind, CacheKind::RawParts);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nonzero_padding_byte_is_reported() {
    let (root, namespace) = production_cache_dir().await;
    let (path, _, found) = first_written_block(&namespace.join(PARTS_CACHE_DIR));
    let padding = found
        .iter()
        .flat_map(|blob| &blob.entries)
        .map(|entry| entry.padding.clone())
        .find(|padding| !padding.is_empty())
        .unwrap();
    plant_byte(&path, padding.start);

    let report = scan(root.path());
    assert_eq!(report.status, ScanStatus::UndefinedBytes);
    assert_eq!(report.status.exit_code(), 3);
    let parts = cache(&report, CacheKind::RawParts).scanned.clone().unwrap();
    assert_eq!((parts.padding_nonzero, parts.index_tail_nonzero), (1, 0));
    let blocks = cache(&report, CacheKind::DecodedBlocks)
        .scanned
        .clone()
        .unwrap();
    assert_eq!(blocks.nonzero(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nonzero_index_tail_byte_is_reported() {
    let (root, namespace) = production_cache_dir().await;
    let (path, _, found) = first_written_block(&namespace.join(BLOCK_CACHE_DIR));
    let tail = found
        .iter()
        .map(|blob| blob.index_tail.clone())
        .find(|tail| !tail.is_empty())
        .unwrap();
    plant_byte(&path, tail.end - 1);

    let report = scan(root.path());
    assert_eq!(report.status, ScanStatus::UndefinedBytes);
    assert_eq!(report.status.exit_code(), 3);
    let blocks = cache(&report, CacheKind::DecodedBlocks)
        .scanned
        .clone()
        .unwrap();
    assert_eq!((blocks.index_tail_nonzero, blocks.padding_nonzero), (1, 0));
}

/// Caches opened and closed with nothing written hold no entries. That must
/// not read as clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caches_without_entries_report_nothing_scanned() {
    let root = tempfile::tempdir().unwrap();
    let namespace = root.path().join(NAMESPACE);
    write_parts_cache(&namespace, 0).await;
    write_block_cache(&namespace, 0).await;

    let report = scan(root.path());
    assert_eq!(report.status, ScanStatus::NothingScanned, "{report:#?}");
    assert_eq!(report.status.exit_code(), 4);
    for kind in [CacheKind::RawParts, CacheKind::DecodedBlocks] {
        let counts = cache(&report, kind).scanned.clone().unwrap();
        assert!(counts.partitions > 0);
        assert_eq!(counts.entries, 0);
    }
}

/// A decoded-block cache in the directory older builds used, and a parts
/// cache whose marker predates the zeroed padding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caches_of_earlier_builds_are_reported_and_scanned_only_on_request() {
    let (root, namespace) = production_cache_dir().await;
    std::fs::rename(
        namespace.join(BLOCK_CACHE_DIR),
        namespace.join(LEGACY_BLOCK_CACHE_DIR),
    )
    .unwrap();
    let marker = namespace
        .join(PARTS_CACHE_DIR)
        .join(PARTS_CACHE_LAYOUT_FILE);
    std::fs::write(
        &marker,
        format!(
            "zerofs-parts-cache-v1\nrecover=none\ntombstones=0\nblock_bytes={PARTS_BLOCK_SIZE}\ndisk_bytes={}\n",
            256 * MIB
        ),
    )
    .unwrap();

    let report = scan(root.path());
    assert_eq!(report.status, ScanStatus::NothingScanned, "{report:#?}");
    for kind in [CacheKind::RawParts, CacheKind::DecodedBlocks] {
        let found = cache(&report, kind);
        assert!(
            matches!(found.layout, CacheLayout::Earlier { .. }),
            "{found:#?}"
        );
        assert!(found.scanned.is_none(), "{found:#?}");
    }

    let legacy = scan_cache_files(
        root.path(),
        ScanOptions {
            legacy: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(legacy.status, ScanStatus::Clean, "{legacy:#?}");
    for kind in [CacheKind::RawParts, CacheKind::DecodedBlocks] {
        let found = cache(&legacy, kind);
        assert!(matches!(found.layout, CacheLayout::Earlier { .. }));
        assert!(found.scanned.as_ref().unwrap().entries > 0);
    }
}

/// A server holding the namespace may be appending a blob. The scan
/// refuses unless asked to take a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cache_in_use_by_a_server_is_refused_unless_live() {
    let (root, namespace) = production_cache_dir().await;
    let owner = acquire_clean_cache_owner(&namespace).unwrap();

    let error = scan_cache_files(root.path(), ScanOptions::default()).unwrap_err();
    assert!(
        format!("{error:#}").contains("stop the server"),
        "{error:#}"
    );
    let live = scan_cache_files(
        root.path(),
        ScanOptions {
            live: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(live.live);
    assert_eq!(live.status, ScanStatus::Clean);

    drop(owner);
    assert_eq!(scan(root.path()).status, ScanStatus::Clean);
}
