//! The clean-cache disk tiers write only bytes the cache defines.
//!
//! foyer lays each 64 MiB block out as blobs. A blob is a 4 KiB index
//! (checksum, count, then one 24-byte record per entry) followed by its
//! entries, each a 36-byte header, key and value padded up to the next 4 KiB
//! page. The index tail past the last record and each entry's page padding
//! carry no data. If foyer leaves them as it found them in its I/O buffers,
//! freed heap memory of the server process reaches the cache disk in the
//! clear. These tests fill freed memory with a marker, run both production
//! disk caches over real workloads, and read every cache file back.

use super::{FoyerMetricsRegistry, build_block_hybrid, build_parts_hybrid};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::alloc::{Layout, alloc, dealloc};
use std::sync::Arc;

/// Synthetic stand-in for secret heap contents. Never a real key.
const MARKER: &[u8; 16] = b"ZFS-PLANTED-SEC!";
const PAGE: usize = 4096;
const BLOCK: usize = 64 * 1024 * 1024;
const BLOB_INDEX: usize = 4096;
const BLOB_INDEX_HEAD: usize = 12;
const BLOB_INDEX_RECORD: usize = 24;
const ENTRY_HEADER: usize = 36;
const ENTRY_MAGIC: u32 = 0x9703_2700;
const MIB: usize = 1024 * 1024;

/// Allocate `count` buffers of `size` bytes at `align`, fill them with the
/// marker, then free them all, so the allocator holds freed memory that
/// reads as the marker in that size class.
fn plant_marker(size: usize, align: usize, count: usize) {
    let layout = Layout::from_size_align(size, align).unwrap();
    let buffers: Vec<*mut u8> = (0..count)
        .map(|_| {
            // SAFETY: the layout has a non-zero size; every byte is written
            // before the pointer escapes, and each is freed with its layout.
            unsafe {
                let ptr = alloc(layout);
                assert!(!ptr.is_null());
                for chunk in std::slice::from_raw_parts_mut(ptr, size).chunks_mut(MARKER.len()) {
                    chunk.copy_from_slice(&MARKER[..chunk.len()]);
                }
                std::hint::black_box(ptr)
            }
        })
        .collect();
    for ptr in buffers {
        // SAFETY: allocated above with this layout.
        unsafe { dealloc(std::hint::black_box(ptr), layout) };
    }
}

/// Freed memory in every size class foyer allocates write buffers from: its
/// 4 KiB blob index (page aligned) and the per-flusher I/O buffers (16 MiB
/// for the block cache, 64 MiB for the parts cache), plus the unaligned
/// 4 KiB class a `Vec` of plaintext would come from.
fn plant_marker_in_foyer_buffer_classes() {
    plant_marker(PAGE, 1, 64);
    plant_marker(PAGE, PAGE, 64);
    plant_marker(16 * MIB, PAGE, 4);
    plant_marker(64 * MIB, PAGE, 4);
}

#[derive(Debug, Default)]
struct CacheFileAudit {
    files: usize,
    blocks: usize,
    blobs: usize,
    entries: usize,
    /// Non-zero bytes past the last record of a blob index.
    index_tail_nonzero: usize,
    /// Non-zero bytes between an entry's end and its page boundary.
    entry_padding_nonzero: usize,
    /// Marker copies anywhere in any cache file.
    marker_hits: usize,
    /// Marker copies inside a blob index tail.
    index_tail_marker_hits: usize,
    /// Marker copies inside entry padding.
    entry_padding_marker_hits: usize,
    /// Hex of the first undefined bytes found, to tell what they hold.
    samples: Vec<String>,
}

fn sample(audit: &mut CacheFileAudit, region: &str, bytes: &[u8]) {
    if let Some(first) = bytes.iter().position(|b| *b != 0)
        && audit.samples.len() < 6
    {
        let hex: String = bytes[first..bytes.len().min(first + 48)]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        audit
            .samples
            .push(format!("{region}+{first}/{}: {hex}", bytes.len()));
    }
}

fn count_marker(bytes: &[u8]) -> usize {
    bytes.windows(MARKER.len()).filter(|w| w == MARKER).count()
}

fn count_nonzero(bytes: &[u8]) -> usize {
    bytes.iter().filter(|b| **b != 0).count()
}

fn be_u32(bytes: &[u8], at: usize) -> usize {
    u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
}

/// Walk one block's blobs the way foyer's scanner does and audit the bytes
/// each blob carries but does not define.
fn audit_block(block: &[u8], audit: &mut CacheFileAudit) {
    let mut blob = 0;
    while blob + BLOB_INDEX <= block.len() {
        let index = &block[blob..blob + BLOB_INDEX];
        let count = be_u32(index, 8);
        let capacity = (BLOB_INDEX - BLOB_INDEX_HEAD) / BLOB_INDEX_RECORD;
        if count == 0 || count > capacity {
            return;
        }
        let records = BLOB_INDEX_HEAD + count * BLOB_INDEX_RECORD;
        let mut entries = Vec::with_capacity(count);
        let (index_records, _) = index[BLOB_INDEX_HEAD..records].as_chunks::<BLOB_INDEX_RECORD>();
        for record in index_records {
            let offset = be_u32(record, 16);
            let len = be_u32(record, 20);
            let start = blob + offset;
            if len < ENTRY_HEADER
                || start + len.next_multiple_of(PAGE) > block.len()
                || be_u32(block, start + 32) & 0xFFFF_FF00 != ENTRY_MAGIC as usize
            {
                // Not a blob this pass wrote.
                return;
            }
            entries.push((start, len));
        }
        audit.blobs += 1;
        let tail = &index[records..];
        audit.index_tail_nonzero += count_nonzero(tail);
        audit.index_tail_marker_hits += count_marker(tail);
        sample(audit, "index tail", tail);
        let mut end = blob;
        for (start, len) in entries {
            audit.entries += 1;
            let padding = &block[start + len..start + len.next_multiple_of(PAGE)];
            audit.entry_padding_nonzero += count_nonzero(padding);
            audit.entry_padding_marker_hits += count_marker(padding);
            sample(audit, "entry padding", padding);
            end = start + len.next_multiple_of(PAGE);
        }
        blob = end;
    }
}

/// Read every foyer partition file under `root` back from disk.
fn audit_cache_files(root: &std::path::Path) -> CacheFileAudit {
    let mut audit = CacheFileAudit::default();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                pending.push(path);
                continue;
            }
            let name = entry.file_name();
            if !name
                .to_str()
                .is_some_and(|name| name.starts_with(super::FOYER_PARTITION_PREFIX))
            {
                continue;
            }
            let bytes = std::fs::read(&path).unwrap();
            audit.files += 1;
            audit.marker_hits += count_marker(&bytes);
            if bytes.len() == BLOCK {
                audit.blocks += 1;
                audit_block(&bytes, &mut audit);
            }
        }
    }
    audit
}

fn assert_clean(name: &str, audit: &CacheFileAudit) {
    println!("CACHE_FILE_AUDIT {name} {audit:?}");
    assert!(audit.entries > 0, "{name}: the workload wrote no entries");
    assert_eq!(
        (
            audit.marker_hits,
            audit.index_tail_nonzero,
            audit.entry_padding_nonzero
        ),
        (0, 0, 0),
        "{name}: cache files carry bytes foyer did not define: {audit:?}"
    );
}

/// The raw-parts cache as the server builds it, filled through the
/// production prefetching store by reading objects from the backend.
#[tokio::test(flavor = "current_thread")]
async fn parts_cache_files_hold_no_undefined_bytes() {
    plant_marker_in_foyer_buffer_classes();
    let dir = tempfile::tempdir().unwrap();
    // A memory tier smaller than one object evicts on every insert, so foyer
    // flushes in many batches and reuses each of its rotating I/O buffers.
    let parts = build_parts_hybrid(
        dir.path(),
        MIB,
        256 * MIB,
        &tokio::runtime::Handle::current(),
        FoyerMetricsRegistry::default(),
    )
    .await
    .unwrap();

    let backend = Arc::new(object_store::memory::InMemory::new());
    let store =
        crate::object_store_prefetch::PrefetchingObjectStore::new(backend.clone(), parts.clone());
    for object in 0..24u8 {
        let path = Path::from(format!("segments/audit/{object}"));
        // Uneven tails move the entry boundaries from batch to batch.
        let len = MIB + 1_000 * usize::from(object) + 777;
        let bytes = Bytes::from(vec![object.wrapping_add(1); len]);
        backend
            .put(&path, PutPayload::from_bytes(bytes.clone()))
            .await
            .unwrap();
        let read = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(read, bytes);
        tokio::task::yield_now().await;
    }
    parts.close().await.unwrap();
    drop(store);
    drop(parts);

    assert_clean("raw-parts", &audit_cache_files(dir.path()));
}

/// Enough rows for many 32 KiB SST blocks, so a one-MiB memory tier evicts
/// throughout the reads.
const ROWS: u32 = 20_000;

/// Rows of uneven length so block sizes, and so padding offsets, vary.
fn row(key: u32) -> Vec<u8> {
    vec![(key % 251) as u8 + 1; 64 + (key % 300) as usize]
}

/// The decoded-block cache as the server builds it, filled by SlateDB reads
/// over encrypted SSTs.
#[tokio::test(flavor = "current_thread")]
async fn decoded_block_cache_files_hold_no_undefined_bytes() {
    use slatedb::config::WriteOptions;

    plant_marker_in_foyer_buffer_classes();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let transformer = crate::block_transformer::ZeroFsBlockTransformer::new_arc(
        &[7; 32],
        crate::config::CompressionConfig::default(),
    );
    let open = |cache| {
        slatedb::DbBuilder::new(Path::from("db"), store.clone())
            .with_settings(slatedb::config::Settings {
                compactor_options: None,
                ..Default::default()
            })
            .with_db_cache(cache)
            .with_sst_block_size(slatedb::SstBlockSize::Block32Kib)
            .with_block_transformer(transformer.clone())
            .build()
    };

    let (writer_cache, writer_hybrid) = build_block_hybrid(
        &dir.path().join("writer"),
        64 * MIB,
        512 * MIB,
        &tokio::runtime::Handle::current(),
        FoyerMetricsRegistry::default(),
    )
    .await
    .unwrap();
    let db = open(writer_cache).await.unwrap();
    for key in 0..ROWS {
        db.put_with_options(
            key.to_be_bytes(),
            row(key),
            &slatedb::config::PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();
    writer_hybrid.close().await.unwrap();

    // A fresh cache reads every block from the object store and caches it.
    // A one-MiB memory tier evicts as the reads go, so foyer flushes in
    // many batches and reuses its I/O buffers.
    let (reader_cache, reader_hybrid) = build_block_hybrid(
        &dir.path().join("reader"),
        MIB,
        512 * MIB,
        &tokio::runtime::Handle::current(),
        FoyerMetricsRegistry::default(),
    )
    .await
    .unwrap();
    let db = open(reader_cache).await.unwrap();
    for key in 0..ROWS {
        let value = db.get(key.to_be_bytes()).await.unwrap().expect("row");
        assert_eq!(value.as_ref(), row(key).as_slice());
    }
    db.close().await.unwrap();
    reader_hybrid.close().await.unwrap();

    assert_clean("decoded-blocks", &audit_cache_files(dir.path()));
}
