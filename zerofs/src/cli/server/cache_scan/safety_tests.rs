use super::*;
use std::fs::{File, OpenOptions};
use std::io::Write;

fn put_u32(bytes: &mut [u8], offset: usize, value: usize) {
    bytes[offset..offset + 4].copy_from_slice(&(value as u32).to_be_bytes());
}

/// A minimal blob with one three-byte payload, using the production layout.
fn blob(bytes: &mut [u8], offset: usize) {
    put_u32(bytes, offset + 8, 1);
    put_u32(bytes, offset + BLOB_INDEX_HEAD + 16, BLOB_INDEX);
    put_u32(bytes, offset + BLOB_INDEX_HEAD + 20, ENTRY_HEADER + 3);
    let entry = offset + BLOB_INDEX;
    put_u32(bytes, entry + 4, 3);
    put_u32(bytes, entry + 32, ENTRY_MAGIC as usize);
    bytes[entry + ENTRY_HEADER..entry + ENTRY_HEADER + 3].copy_from_slice(b"abc");
}

fn parts_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let root = tempfile::tempdir().unwrap();
    File::create(root.path().join(super::super::CLEAN_CACHE_LOCK_FILE)).unwrap();
    let parts = root
        .path()
        .join(crate::object_store_prefetch::PARTS_CACHE_DIR);
    std::fs::create_dir(&parts).unwrap();
    std::fs::write(
        parts.join(crate::object_store_prefetch::PARTS_CACHE_LAYOUT_FILE),
        super::super::parts_cache_layout(256 * 1024 * 1024),
    )
    .unwrap();
    let partition = parts.join("foyer-storage-direct-fs-00000000");
    (root, parts, partition)
}

#[test]
fn a_torn_later_blob_cannot_make_a_partial_scan_look_clean() {
    let (root, _, partition) = parts_fixture();
    let mut bytes = vec![0; 4 * PAGE];
    blob(&mut bytes, 0);
    blob(&mut bytes, 2 * PAGE);
    // The published second index names an entry whose header is torn. The
    // first blob alone used to make the entire cache report clean.
    bytes[3 * PAGE + 32] = 0;
    let mut file = File::create(partition).unwrap();
    file.write_all(&bytes).unwrap();
    file.set_len(super::super::PARTS_BLOCK_SIZE as u64).unwrap();
    drop(file);

    let report = scan_cache_files(root.path(), ScanOptions::default()).unwrap();
    assert_ne!(report.status, ScanStatus::Clean, "{report:#?}");
}

#[test]
fn nonzero_unparseable_index_tails_are_not_clean_terminators() {
    for oversized_count in [false, true] {
        let (root, _, partition) = parts_fixture();
        let mut bytes = vec![0; 3 * PAGE];
        blob(&mut bytes, 0);
        if oversized_count {
            let capacity = (BLOB_INDEX - BLOB_INDEX_HEAD) / BLOB_INDEX_RECORD;
            put_u32(&mut bytes, 2 * PAGE + 8, capacity + 1);
        } else {
            // Even a zero count is not a clean terminator if the alleged
            // index page still holds other bytes that cannot be inspected.
            bytes[2 * PAGE + BLOB_INDEX_HEAD] = 0x5a;
        }
        let mut file = File::create(partition).unwrap();
        file.write_all(&bytes).unwrap();
        file.set_len(super::super::PARTS_BLOCK_SIZE as u64).unwrap();
        drop(file);

        let report = scan_cache_files(root.path(), ScanOptions::default()).unwrap();
        assert_ne!(report.status, ScanStatus::Clean, "{report:#?}");
    }
}

#[test]
fn an_index_length_cannot_hide_nonzero_entry_padding() {
    let (root, _, partition) = parts_fixture();
    let mut bytes = vec![0; 2 * PAGE];
    blob(&mut bytes, 0);
    bytes[PAGE + ENTRY_HEADER + 3] = 0x5a;
    // The header still describes three value bytes; trusting only the index
    // length would move the planted padding byte into the apparent payload.
    put_u32(&mut bytes, BLOB_INDEX_HEAD + 20, ENTRY_HEADER + 4);
    let mut file = File::create(partition).unwrap();
    file.write_all(&bytes).unwrap();
    file.set_len(super::super::PARTS_BLOCK_SIZE as u64).unwrap();
    drop(file);

    let report = scan_cache_files(root.path(), ScanOptions::default()).unwrap();
    assert_ne!(report.status, ScanStatus::Clean, "{report:#?}");
}

#[test]
fn a_direct_cache_does_not_hide_a_cache_in_a_sibling_namespace() {
    let (root, _, direct_partition) = parts_fixture();
    let mut bytes = vec![0; 2 * PAGE];
    blob(&mut bytes, 0);
    let mut file = File::create(direct_partition).unwrap();
    file.write_all(&bytes).unwrap();
    file.set_len(super::super::PARTS_BLOCK_SIZE as u64).unwrap();
    drop(file);

    let nested = root
        .path()
        .join("bucket_other")
        .join(crate::object_store_prefetch::PARTS_CACHE_DIR);
    std::fs::create_dir_all(&nested).unwrap();
    File::create(
        nested
            .parent()
            .unwrap()
            .join(super::super::CLEAN_CACHE_LOCK_FILE),
    )
    .unwrap();
    std::fs::write(
        nested.join(crate::object_store_prefetch::PARTS_CACHE_LAYOUT_FILE),
        super::super::parts_cache_layout(256 * 1024 * 1024),
    )
    .unwrap();
    bytes[PAGE + ENTRY_HEADER + 3] = 0x5a;
    let mut file = File::create(nested.join("foyer-storage-direct-fs-00000000")).unwrap();
    file.write_all(&bytes).unwrap();
    file.set_len(super::super::PARTS_BLOCK_SIZE as u64).unwrap();
    drop(file);

    let report = scan_cache_files(root.path(), ScanOptions::default()).unwrap();
    assert_eq!(report.status, ScanStatus::UndefinedBytes, "{report:#?}");
    assert_eq!(report.caches.len(), 2);
}

#[test]
fn a_server_cannot_acquire_the_namespace_during_an_offline_scan() {
    use fs4::fs_std::FileExt;
    let (root, _, partition) = parts_fixture();
    File::create(partition).unwrap();
    let lock_path = root.path().join(super::super::CLEAN_CACHE_LOCK_FILE);
    let contender = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();

    scan_cache_files_with_hook(root.path(), ScanOptions::default(), || {
        assert!(
            !FileExt::try_lock_exclusive(&contender).unwrap(),
            "a server can begin writing while the offline scan is reading"
        );
    })
    .unwrap();
    assert!(FileExt::try_lock_exclusive(&contender).unwrap());
    FileExt::unlock(&contender).unwrap();
}

#[test]
fn an_offline_scan_requires_an_existing_ownership_lock() {
    let (root, _, partition) = parts_fixture();
    File::create(partition).unwrap();
    let lock_path = root.path().join(super::super::CLEAN_CACHE_LOCK_FILE);
    std::fs::remove_file(&lock_path).unwrap();

    let error = scan_cache_files(root.path(), ScanOptions::default()).unwrap_err();
    assert!(format!("{error:#}").contains("--live"), "{error:#}");
    assert!(
        !lock_path.exists(),
        "a read-only scan must not create a lock"
    );
    let report = scan_cache_files(
        root.path(),
        ScanOptions {
            live: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(report.live);
    assert_eq!(report.status, ScanStatus::NothingScanned);
}

/// A damaged apparent file size must not turn this read-only diagnostic into
/// an unbounded allocation. Use a child with an address-space limit so the
/// regression fails safely instead of exhausting the test machine.
#[cfg(target_os = "linux")]
#[test]
fn a_wrong_sized_partition_is_skipped_without_allocating_its_length() {
    const CHILD: &str = "ZEROFS_CACHE_SCAN_SPARSE_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        let limit = libc::rlimit {
            rlim_cur: 512 * 1024 * 1024,
            rlim_max: 512 * 1024 * 1024,
        };
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) }, 0);
        let counts = scan_partitions(Path::new(&path), super::super::PARTS_BLOCK_SIZE, |_, _| {
            panic!("wrong-sized partitions must not be read as blocks")
        })
        .unwrap();
        assert_eq!(counts.other_partitions, 1);
        assert_eq!(counts.partitions, 0);
        return;
    }
    let (root, parts, partition) = parts_fixture();
    File::create(partition).unwrap().set_len(1 << 34).unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "a_wrong_sized_partition_is_skipped_without_allocating_its_length",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, parts)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "bounded sparse-file scan failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    drop(root);
}
