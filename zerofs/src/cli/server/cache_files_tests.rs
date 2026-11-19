//! The clean-cache disk tiers write only bytes the cache defines.
//!
//! The index tail past each blob's last record and each entry's page padding
//! carry no data (see [`super::cache_scan`]). If foyer leaves them as it found
//! them in its I/O buffers, freed heap memory of the server process reaches
//! the cache disk in the clear. These tests fill freed memory with a marker,
//! run both production disk caches over real workloads, and read every cache
//! file back with the same scanner `zerofs debug scan-cache-files` uses.

use super::cache_scan::{UndefinedBytes, scan_partitions};
use super::{
    BLOCK_CACHE_BLOCK_SIZE, FoyerMetricsRegistry, PARTS_BLOCK_SIZE, build_block_hybrid,
    build_parts_hybrid,
};
use crate::object_store_prefetch::PARTS_CACHE_DIR;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::alloc::{Layout, alloc, dealloc};
use std::sync::Arc;

/// Synthetic stand-in for secret heap contents. Never a real key.
const MARKER: &[u8; 16] = b"ZFS-PLANTED-SEC!";
const PAGE: usize = 4096;
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
    undefined: UndefinedBytes,
    /// Marker copies anywhere in any cache block.
    marker_hits: usize,
    /// Marker copies inside a blob index tail.
    index_tail_marker_hits: usize,
    /// Marker copies inside entry padding.
    entry_padding_marker_hits: usize,
    /// Hex of the first undefined bytes found, to tell what they hold.
    samples: Vec<String>,
}

fn sample(samples: &mut Vec<String>, region: &str, bytes: &[u8]) {
    if let Some(first) = bytes.iter().position(|b| *b != 0)
        && samples.len() < 6
    {
        let hex: String = bytes[first..bytes.len().min(first + 48)]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        samples.push(format!("{region}+{first}/{}: {hex}", bytes.len()));
    }
}

fn count_marker(bytes: &[u8]) -> usize {
    bytes.windows(MARKER.len()).filter(|w| w == MARKER).count()
}

/// Scan one cache directory and add what it holds to `audit`.
fn audit_cache_dir(dir: &std::path::Path, block_size: usize, audit: &mut CacheFileAudit) {
    let mut marker_hits = 0;
    let mut tail_hits = 0;
    let mut padding_hits = 0;
    let mut samples = std::mem::take(&mut audit.samples);
    let counts = scan_partitions(dir, block_size, |block, blobs| {
        marker_hits += count_marker(block);
        for blob in blobs {
            let tail = &block[blob.index_tail.clone()];
            tail_hits += count_marker(tail);
            sample(&mut samples, "index tail", tail);
            for entry in &blob.entries {
                let padding = &block[entry.padding.clone()];
                padding_hits += count_marker(padding);
                sample(&mut samples, "entry padding", padding);
            }
        }
    })
    .unwrap();
    let total = &mut audit.undefined;
    total.partitions += counts.partitions;
    total.other_partitions += counts.other_partitions;
    total.incomplete_partitions += counts.incomplete_partitions;
    total.blobs += counts.blobs;
    total.entries += counts.entries;
    total.index_tail_bytes += counts.index_tail_bytes;
    total.index_tail_nonzero += counts.index_tail_nonzero;
    total.padding_bytes += counts.padding_bytes;
    total.padding_nonzero += counts.padding_nonzero;
    audit.marker_hits += marker_hits;
    audit.index_tail_marker_hits += tail_hits;
    audit.entry_padding_marker_hits += padding_hits;
    audit.samples = samples;
}

fn assert_clean(name: &str, audit: &CacheFileAudit) {
    println!("CACHE_FILE_AUDIT {name} {audit:?}");
    assert!(
        audit.undefined.entries > 0,
        "{name}: the workload wrote no entries"
    );
    assert_eq!(
        (
            audit.marker_hits,
            audit.undefined.index_tail_nonzero,
            audit.undefined.padding_nonzero,
            audit.undefined.incomplete_partitions
        ),
        (0, 0, 0, 0),
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

    let mut audit = CacheFileAudit::default();
    audit_cache_dir(
        &dir.path().join(PARTS_CACHE_DIR),
        PARTS_BLOCK_SIZE,
        &mut audit,
    );
    assert_clean("raw-parts", &audit);
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

    let mut audit = CacheFileAudit::default();
    for cache in ["writer", "reader"] {
        audit_cache_dir(&dir.path().join(cache), BLOCK_CACHE_BLOCK_SIZE, &mut audit);
    }
    assert_clean("decoded-blocks", &audit);
}
