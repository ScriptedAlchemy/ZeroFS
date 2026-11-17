//! Reads clean-cache partition files back and finds the bytes foyer writes
//! without defining them.
//!
//! foyer lays each block out as blobs. A blob is a 4 KiB index (checksum,
//! count, then one 24-byte record per entry) followed by its entries, each a
//! 36-byte header, key and value padded up to the next 4 KiB page. The index
//! tail past the last record and each entry's page padding carry no data, so
//! with the patched foyer-storage they are zero. This is the only description
//! of that layout in ZeroFS; the library tests and the scan report use it.

use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::ops::Range;
use std::path::Path;

const PAGE: usize = 4096;
const BLOB_INDEX: usize = 4096;
const BLOB_INDEX_HEAD: usize = 12;
const BLOB_INDEX_RECORD: usize = 24;
const ENTRY_HEADER: usize = 36;
const ENTRY_MAGIC: u32 = 0x9703_2700;
const ENTRY_MAGIC_MASK: u32 = 0xFFFF_FF00;

/// One blob of a block, as byte ranges within the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobLayout {
    /// The index bytes past the last record.
    pub(crate) index_tail: Range<usize>,
    pub(crate) entries: Vec<EntryLayout>,
}

/// One entry of a blob, as byte ranges within the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntryLayout {
    /// Header, key and value.
    pub(crate) data: Range<usize>,
    /// From the end of the data to the next 4 KiB page.
    pub(crate) padding: Range<usize>,
}

fn be_u32(bytes: &[u8], at: usize) -> usize {
    u32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes")) as usize
}

/// Walk one block's blobs the way foyer's recovery scanner does: from the
/// start, one blob after another, until an index does not describe entries
/// with valid headers.
#[cfg(test)]
pub(crate) fn blobs(block: &[u8]) -> Vec<BlobLayout> {
    inspect_blobs(block).0
}

/// Also report a recognizable index whose entries cannot be inspected. A
/// valid prefix of a torn blob chain is not evidence for the whole block.
fn inspect_blobs(block: &[u8]) -> (Vec<BlobLayout>, bool) {
    let mut found = Vec::new();
    let mut blob = 0;
    while blob + BLOB_INDEX <= block.len() {
        let index = &block[blob..blob + BLOB_INDEX];
        let count = be_u32(index, 8);
        let capacity = (BLOB_INDEX - BLOB_INDEX_HEAD) / BLOB_INDEX_RECORD;
        if count == 0 {
            // Fresh or explicitly cleaned blocks end at a zero index page.
            // Reused/torn bytes that merely decode a zero count do not prove
            // that everything after the valid prefix was inspected.
            return (found, index.iter().any(|byte| *byte != 0));
        }
        if count > capacity {
            return (found, true);
        }
        let records = BLOB_INDEX_HEAD + count * BLOB_INDEX_RECORD;
        let (index_records, _) = index[BLOB_INDEX_HEAD..records].as_chunks::<BLOB_INDEX_RECORD>();
        let mut entries = Vec::with_capacity(count);
        let mut next = blob + BLOB_INDEX;
        for record in index_records {
            let Some(start) = blob.checked_add(be_u32(record, 16)) else {
                return (found, true);
            };
            let len = be_u32(record, 20);
            // Foyer packs aligned entries consecutively after their index.
            // Refuse overlapping, out-of-order, and out-of-bounds records
            // before using any file-controlled value as a slice boundary.
            if start != next || len < ENTRY_HEADER || len > block.len().saturating_sub(start) {
                return (found, true);
            }
            let data_end = start + len;
            let Some(end) = data_end.checked_add(PAGE - 1).map(|end| end / PAGE * PAGE) else {
                return (found, true);
            };
            let header = &block[start..start + ENTRY_HEADER];
            let header_len = be_u32(header, 0)
                .checked_add(be_u32(header, 4))
                .and_then(|len| len.checked_add(ENTRY_HEADER));
            if end > block.len()
                || be_u32(header, 32) as u32 & ENTRY_MAGIC_MASK != ENTRY_MAGIC
                || header_len != Some(len)
                || header[8..24] != record[..16]
            {
                return (found, true);
            }
            entries.push(EntryLayout {
                data: start..data_end,
                padding: data_end..end,
            });
            next = end;
        }
        found.push(BlobLayout {
            index_tail: blob + records..blob + BLOB_INDEX,
            entries,
        });
        blob = next;
    }
    (found, false)
}

/// Counts of undefined bytes in one cache directory's partitions.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct UndefinedBytes {
    /// Partition files of the cache's block size that were read.
    pub(crate) partitions: usize,
    /// Partition files of another size, which were not read as data blocks.
    pub(crate) other_partitions: usize,
    /// Partitions with invalid geometry or a blob that could not be fully inspected.
    pub(crate) incomplete_partitions: usize,
    pub(crate) blobs: usize,
    pub(crate) entries: usize,
    pub(crate) index_tail_bytes: usize,
    pub(crate) index_tail_nonzero: usize,
    pub(crate) padding_bytes: usize,
    pub(crate) padding_nonzero: usize,
}

impl UndefinedBytes {
    pub(crate) fn nonzero(&self) -> usize {
        self.index_tail_nonzero + self.padding_nonzero
    }
}

fn count_nonzero(bytes: &[u8]) -> usize {
    bytes.iter().filter(|b| **b != 0).count()
}

fn open_regular_read_only(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // NOFOLLOW checks the file actually opened; NONBLOCK keeps a raced
        // replacement with a FIFO from hanging before the type check below.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    if !file.metadata()?.file_type().is_file() {
        anyhow::bail!("{} is not a regular file", path.display());
    }
    Ok(file)
}

/// Read every foyer partition file directly in `dir`, read-only, and count
/// the undefined bytes of every blob in each block of `block_size`.
/// `visit` sees each block with its blobs, for callers that look further.
pub(crate) fn scan_partitions(
    dir: &Path,
    block_size: usize,
    mut visit: impl FnMut(&[u8], &[BlobLayout]),
) -> Result<UndefinedBytes> {
    let mut counts = UndefinedBytes::default();
    let mut partitions = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading cache directory {}", dir.display()))?
    {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .and_then(super::foyer_partition_id)
            .is_none()
        {
            continue;
        }
        let path = entry.path();
        if !std::fs::symlink_metadata(&path)?.file_type().is_file() {
            anyhow::bail!("cache partition {} is not a regular file", path.display());
        }
        partitions.push(path);
    }
    partitions.sort();
    for path in partitions {
        let mut file = open_regular_read_only(&path)?;
        // Inspect geometry before allocating. A damaged or sparse partition
        // can report a size vastly larger than the cache's fixed block size.
        if file.metadata()?.len() != block_size as u64 {
            counts.other_partitions += 1;
            continue;
        }
        let mut block = vec![0; block_size];
        file.read_exact(&mut block)
            .with_context(|| format!("reading cache partition {}", path.display()))?;
        counts.partitions += 1;
        let (found, incomplete) = inspect_blobs(&block);
        counts.incomplete_partitions += usize::from(incomplete);
        for blob in &found {
            counts.blobs += 1;
            let tail = &block[blob.index_tail.clone()];
            counts.index_tail_bytes += tail.len();
            counts.index_tail_nonzero += count_nonzero(tail);
            for entry in &blob.entries {
                let padding = &block[entry.padding.clone()];
                counts.entries += 1;
                counts.padding_bytes += padding.len();
                counts.padding_nonzero += count_nonzero(padding);
            }
        }
        visit(&block, &found);
    }
    Ok(counts)
}

/// Which on-disk cache a directory holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CacheKind {
    RawParts,
    DecodedBlocks,
}

impl CacheKind {
    fn name(self) -> &'static str {
        match self {
            Self::RawParts => "raw-parts cache",
            Self::DecodedBlocks => "decoded-block cache",
        }
    }

    fn block_size(self) -> usize {
        match self {
            Self::RawParts => super::PARTS_BLOCK_SIZE,
            Self::DecodedBlocks => super::BLOCK_CACHE_BLOCK_SIZE,
        }
    }
}

/// The cache directories of the current layouts and of earlier builds, by
/// the names the server gives them.
fn cache_dir_kind(name: &str) -> Option<CacheKind> {
    use crate::object_store_prefetch::{LEGACY_PARTS_CACHE_DIR, PARTS_CACHE_DIR};
    match name {
        PARTS_CACHE_DIR | LEGACY_PARTS_CACHE_DIR => Some(CacheKind::RawParts),
        super::BLOCK_CACHE_DIR | super::LEGACY_BLOCK_CACHE_DIR => Some(CacheKind::DecodedBlocks),
        _ => None,
    }
}

/// Whether a cache directory was written with the zeroed padding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "layout", rename_all = "kebab-case")]
pub(crate) enum CacheLayout {
    Current,
    /// Written by an earlier build, which left padding undefined.
    Earlier {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CacheReport {
    pub(crate) kind: CacheKind,
    pub(crate) path: std::path::PathBuf,
    #[serde(flatten)]
    pub(crate) layout: CacheLayout,
    /// The counts, or nothing for an earlier layout scanned without `--legacy`.
    pub(crate) scanned: Option<UndefinedBytes>,
}

/// The scan's verdict; each has its own process exit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ScanStatus {
    /// Every inspected byte is zero, every cache had entries, and no
    /// recognizable blob index was left incompletely inspected.
    Clean,
    /// Some index tail or padding byte is non-zero.
    UndefinedBytes,
    /// A cache had no entries on disk, was not scanned, or had invalid
    /// geometry or an incomplete blob that could not be fully inspected.
    NothingScanned,
}

impl ScanStatus {
    pub(crate) fn exit_code(self) -> i32 {
        match self {
            Self::Clean => 0,
            Self::UndefinedBytes => 3,
            Self::NothingScanned => 4,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct ScanReport {
    pub(crate) path: std::path::PathBuf,
    /// Scanned while a server may have been writing.
    pub(crate) live: bool,
    pub(crate) caches: Vec<CacheReport>,
    pub(crate) status: ScanStatus,
    /// The process exit status the command returns for `status`.
    pub(crate) exit_status: i32,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScanOptions {
    /// Scan even when a server holds the cache's ownership lock.
    pub(crate) live: bool,
    /// Count the bytes of caches written by earlier builds too.
    pub(crate) legacy: bool,
}

/// The cache directories at or under `path`: one cache directory, a
/// clean-cache namespace holding them, or the `[cache] dir` holding
/// namespaces.
fn find_cache_dirs(path: &Path) -> Result<Vec<(CacheKind, std::path::PathBuf)>> {
    let is_dir = |path: &Path| {
        std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
    };
    let named = |path: &Path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(cache_dir_kind)
    };
    if !is_dir(path) {
        anyhow::bail!("{} is not a directory", path.display());
    }
    if let Some(kind) = named(path) {
        return Ok(vec![(kind, path.to_path_buf())]);
    }
    let children = |dir: &Path| -> Result<Vec<std::path::PathBuf>> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
            let path = entry?.path();
            if is_dir(&path) {
                found.push(path);
            }
        }
        found.sort();
        Ok(found)
    };
    let caches_in = |dir: &Path| -> Result<Vec<(CacheKind, std::path::PathBuf)>> {
        Ok(children(dir)?
            .into_iter()
            .filter_map(|child| named(&child).map(|kind| (kind, child)))
            .collect())
    };
    let mut found = Vec::new();
    for namespace in children(path)? {
        if let Some(kind) = named(&namespace) {
            found.push((kind, namespace));
        } else {
            found.extend(caches_in(&namespace)?);
        }
    }
    if found.is_empty() {
        anyhow::bail!(
            "no ZeroFS clean-cache directory found at or under {}; pass the [cache] dir, \
             one of its namespaces, or a cache directory inside one",
            path.display()
        );
    }
    Ok(found)
}

/// The layout `dir` was written with, from its name and, for the parts
/// cache, the layout marker the server writes.
fn layout_of(kind: CacheKind, dir: &Path) -> Result<CacheLayout> {
    use crate::object_store_prefetch::{LEGACY_PARTS_CACHE_DIR, PARTS_CACHE_LAYOUT_FILE};
    let earlier = |reason: String| Ok(CacheLayout::Earlier { reason });
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    match kind {
        CacheKind::DecodedBlocks if name == super::BLOCK_CACHE_DIR => Ok(CacheLayout::Current),
        CacheKind::DecodedBlocks => earlier(format!(
            "{name} is where builds before the zeroed padding kept this cache"
        )),
        CacheKind::RawParts if name == LEGACY_PARTS_CACHE_DIR => earlier(format!(
            "{name} is where builds before the cold-start layout kept this cache"
        )),
        CacheKind::RawParts => {
            let marker = dir.join(PARTS_CACHE_LAYOUT_FILE);
            let text = match std::fs::symlink_metadata(&marker) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return earlier("it has no layout marker".to_owned());
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading layout marker {}", marker.display()));
                }
                Ok(metadata) if !metadata.file_type().is_file() => {
                    anyhow::bail!("layout marker {} is not a regular file", marker.display());
                }
                Ok(_) => {
                    let mut text = String::new();
                    open_regular_read_only(&marker)?
                        .take(super::parts_cache_layout(usize::MAX).len() as u64 + 1)
                        .read_to_string(&mut text)
                        .with_context(|| format!("reading layout marker {}", marker.display()))?;
                    text
                }
            };
            let disk_bytes = text
                .lines()
                .find_map(|line| line.strip_prefix("disk_bytes="))
                .and_then(|value| value.parse().ok());
            if disk_bytes.is_some_and(|disk_bytes| text == super::parts_cache_layout(disk_bytes)) {
                Ok(CacheLayout::Current)
            } else if !text.contains(super::CACHE_PADDING_POLICY) {
                earlier(format!(
                    "its layout marker does not record {}",
                    super::CACHE_PADDING_POLICY.trim_end()
                ))
            } else {
                earlier("its layout marker does not match this build's layout".to_owned())
            }
        }
    }
}

struct ScanLock(File);

impl Drop for ScanLock {
    fn drop(&mut self) {
        // Release explicitly even if a forked child inherited this file.
        let _ = fs4::fs_std::FileExt::unlock(&self.0);
    }
}

/// Keep the namespace's existing lock shared for the entire scan, so a
/// server cannot start changing the files between the probe and the reads.
fn lock_namespace_for_scan(namespace: &Path) -> Result<ScanLock> {
    use fs4::fs_std::FileExt;
    let lock = namespace.join(super::CLEAN_CACHE_LOCK_FILE);
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(&lock) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!(
                "{} has no cache ownership lock {}; an offline scan cannot prevent a server \
                 from starting there. Pass --live to scan this unlocked or copied cache as a \
                 snapshot",
                namespace.display(),
                lock.display()
            );
        }
        Err(error) => {
            return Err(error).with_context(|| format!("opening {}", lock.display()));
        }
    };
    if !file.metadata()?.file_type().is_file() {
        anyhow::bail!("{} is not a regular file", lock.display());
    }
    if !FileExt::try_lock_shared(&file).with_context(|| format!("probing {}", lock.display()))? {
        anyhow::bail!(
            "a running ZeroFS server is using {} (it holds {}); stop the server and scan \
             again, or pass --live to scan a snapshot in which a blob being written can \
             look torn",
            namespace.display(),
            lock.display()
        );
    }
    Ok(ScanLock(file))
}

fn has_partitions(dir: &Path) -> Result<bool> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading cache directory {}", dir.display()))?
    {
        if entry?
            .file_name()
            .to_str()
            .and_then(super::foyer_partition_id)
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Scan the clean-cache files at or under `path`. Opens files read-only,
/// changes nothing, and needs no password or key: the check is that bytes
/// carrying no data are zero, which needs no decryption.
pub(crate) fn scan_cache_files(path: &Path, options: ScanOptions) -> Result<ScanReport> {
    scan_cache_files_with_hook(path, options, || {})
}

fn scan_cache_files_with_hook(
    path: &Path,
    options: ScanOptions,
    before_read: impl FnOnce(),
) -> Result<ScanReport> {
    let found = find_cache_dirs(path)?;
    let mut locks = Vec::new();
    if !options.live {
        let mut namespaces: Vec<_> = found
            .iter()
            .filter_map(|(_, dir)| dir.parent().map(Path::to_path_buf))
            .collect();
        namespaces.sort();
        namespaces.dedup();
        for namespace in namespaces {
            locks.push(lock_namespace_for_scan(&namespace)?);
        }
    }
    before_read();
    let mut caches = Vec::new();
    for (kind, dir) in found {
        let layout = layout_of(kind, &dir)?;
        let current = layout == CacheLayout::Current;
        // A server that starts with the fix removes earlier caches' partitions
        // and leaves the empty directory; there is nothing left to report.
        if !current && !has_partitions(&dir)? {
            continue;
        }
        let scanned = if current || options.legacy {
            let mut counts = scan_partitions(&dir, kind.block_size(), |_, _| {})?;
            if current {
                // Neither current layout has a tombstone log. Each partition must be
                // a full data block, so a truncated or foreign-size file is
                // missing evidence, not an auxiliary partition to ignore.
                counts.incomplete_partitions += counts.other_partitions;
            }
            Some(counts)
        } else {
            None
        };
        caches.push(CacheReport {
            kind,
            path: dir,
            layout,
            scanned,
        });
    }
    let status = if caches
        .iter()
        .any(|cache| cache.scanned.as_ref().is_some_and(|c| c.nonzero() > 0))
    {
        ScanStatus::UndefinedBytes
    } else if caches.is_empty()
        || caches.iter().any(|cache| {
            cache
                .scanned
                .as_ref()
                .is_none_or(|c| c.entries == 0 || c.incomplete_partitions > 0)
        })
    {
        ScanStatus::NothingScanned
    } else {
        ScanStatus::Clean
    };
    Ok(ScanReport {
        path: path.to_path_buf(),
        live: options.live,
        caches,
        status,
        exit_status: status.exit_code(),
    })
}

/// The report for a person reading a terminal.
pub(crate) fn render(report: &ScanReport) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Clean-cache scan of {} (read-only; no password or key used)",
        report.path.display()
    );
    if report.live {
        let _ = writeln!(
            out,
            "Scanned live: a snapshot taken while a server may have been writing."
        );
    }
    for cache in &report.caches {
        let layout = match &cache.layout {
            CacheLayout::Current => "current layout".to_owned(),
            CacheLayout::Earlier { reason } => {
                format!("written by an earlier build, not covered by the fix: {reason}")
            }
        };
        let _ = writeln!(
            out,
            "\n{}: {} ({layout})",
            cache.kind.name(),
            cache.path.display()
        );
        let Some(counts) = &cache.scanned else {
            let _ = writeln!(
                out,
                "  not scanned; pass --legacy to count its bytes. A server with the fix \
                 removes it when it starts."
            );
            continue;
        };
        let _ = writeln!(
            out,
            "  partitions {}, blobs {}, entries {}",
            counts.partitions, counts.blobs, counts.entries
        );
        let _ = writeln!(
            out,
            "  index tails:   {} bytes scanned, {} non-zero",
            counts.index_tail_bytes, counts.index_tail_nonzero
        );
        let _ = writeln!(
            out,
            "  entry padding: {} bytes scanned, {} non-zero",
            counts.padding_bytes, counts.padding_nonzero
        );
        if counts.entries == 0 {
            let _ = writeln!(
                out,
                "  no entries on disk: nothing was scanned, so this is not evidence that \
                 the cache is clean"
            );
        }
        if counts.incomplete_partitions > 0 {
            let _ = writeln!(
                out,
                "  {} partitions have invalid geometry or an incomplete blob; their remaining \
                 entries could not be scanned, so this is not evidence that the cache is clean",
                counts.incomplete_partitions
            );
        }
    }
    let verdict = match report.status {
        ScanStatus::Clean => {
            "clean: every scanned index-tail and padding byte is zero, and every cache had \
             entries to scan with no incomplete blobs"
        }
        ScanStatus::UndefinedBytes => {
            "undefined bytes found: some index-tail or padding bytes are not zero"
        }
        ScanStatus::NothingScanned => {
            "not proven clean: a cache had no entries on disk or was not completely scanned"
        }
    };
    let _ = writeln!(
        out,
        "\nResult: {verdict} (exit status {}).",
        report.exit_status
    );
    out
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod safety_tests;
