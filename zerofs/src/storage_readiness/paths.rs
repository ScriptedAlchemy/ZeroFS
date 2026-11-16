//! Checks for stores whose APIs still open filesystem paths.

use super::config::absolute_path;
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

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

/// Include dynamic object keys, cache partitions, and journal blob shards.
/// Missing trees are left for the ordinary open or strict root check to handle.
pub(crate) fn verify_tree(root: &Path) -> Result<()> {
    crate::writeback::anchored_dir::AnchoredDir::reject_symlinks_below(root)
}
