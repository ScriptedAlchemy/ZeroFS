//! Checks for stores whose APIs still open filesystem paths.

use super::config::absolute_path;
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Inspect each existing component below the declared root, including the leaf.
/// This is a preflight, not a descriptor-pinned open. A later replacement can race it.
pub(crate) fn existing_ancestor(root: &Path, path: &Path) -> Result<PathBuf> {
    absolute_path(path)?;
    let relative = path
        .strip_prefix(root)
        .context("readiness open path escapes its declared role")?;
    let mut existing = root.to_path_buf();
    for component in relative.components() {
        let next = existing.join(component);
        match next.symlink_metadata() {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("readiness path contains a symlink: {}", next.display());
            }
            Ok(_) => existing = next,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error).context("cannot inspect readiness open path"),
        }
    }
    Ok(existing)
}

pub(super) fn verify_role(root: &super::config::StorageRoot<'_>) -> Result<()> {
    use super::devices::RoleName;
    match std::fs::metadata(&root.path) {
        Ok(metadata) if !metadata.is_dir() => {
            bail!("readiness {} root is not a directory", root.role)
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect readiness role root"),
    }
    let children: &[&str] = match root.role {
        RoleName::Authority => &[
            ".zerofs-pool/identity",
            ".zerofs-pool/.zerofs-local-mutations#0",
            ".zerofs_bucket_id",
            "zerofs.key",
            ".zerofs_ha_leader",
            ".zerofs_ha_ownership",
            ".zerofs-local-mutations#0",
            "manifest",
            "wal",
            "compacted",
            "segments",
        ],
        RoleName::Catalog => &["pool.lock", "pool.redb"],
        RoleName::Member(_) => &[".zerofs-pool"],
        RoleName::Cache | RoleName::Journal => &[],
    };
    // Startup checks fixed paths and sets bounded by configuration. Never
    // enumerate authority/member objects or journal blobs, whose counts grow
    // with stored data. Object operations check their components before I/O;
    // listings reject symlinks while walking only their requested subtree.
    for child in children {
        existing_ancestor(&root.path, &root.path.join(child))?;
    }
    Ok(())
}

pub(super) fn verify_cache_partitions(cache: &Path) -> Result<()> {
    for directory in [cache.join("hybrid_cache"), cache.join("parts_cache")] {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).context("inspect readiness cache partitions"),
        };
        let mut progress = ScanProgress::new("checking cache partitions", &directory);
        // Cache partition counts are bounded by configuration. Check existing
        // leaves before Foyer opens them, without walking any partition contents.
        for entry in entries {
            progress.entry();
            let entry = entry?;
            let name = entry.file_name();
            if name
                .to_str()
                .and_then(|name| name.strip_prefix("foyer-storage-direct-fs-"))
                .is_some_and(|id| id.len() == 8 && id.bytes().all(|byte| byte.is_ascii_digit()))
            {
                existing_ancestor(cache, &entry.path())?;
            }
        }
    }
    Ok(())
}

pub(crate) struct ScanProgress {
    operation: &'static str,
    root: PathBuf,
    entries: u64,
    next_log: Instant,
}

impl ScanProgress {
    pub(crate) fn new(operation: &'static str, root: &Path) -> Self {
        Self {
            operation,
            root: root.to_owned(),
            entries: 0,
            next_log: Instant::now() + Duration::from_secs(5),
        }
    }

    pub(crate) fn entry(&mut self) {
        self.entries += 1;
        let now = Instant::now();
        if now >= self.next_log {
            tracing::info!(operation = self.operation, root = %self.root.display(), entries = self.entries, "Local directory scan is progressing");
            self.next_log = now + Duration::from_secs(5);
        }
    }
}
