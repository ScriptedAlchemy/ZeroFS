//! Startup readiness only. Passing this preflight never enables placement.

mod config;
#[cfg(any(target_os = "linux", test))]
mod linux;

use crate::config::Settings;
use anyhow::{Context, Result, bail};
pub(crate) use config::ReadinessConfig;
use config::{FilesystemIdentity, absolute_path};
use std::path::{Path, PathBuf};

/// Facts about the filesystem actually containing a role's existing root.
/// Providers must report errors for unknown identity, mount or unlock state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MountedFilesystem {
    mountpoint: PathBuf,
    filesystem_root: PathBuf,
    identity: FilesystemIdentity,
    writable: bool,
    unlocked: bool,
}

pub(super) trait IdentityProvider {
    fn observe(&self, root: &Path) -> Result<MountedFilesystem>;
}

struct HostIdentityProvider;

impl IdentityProvider for HostIdentityProvider {
    fn observe(&self, root: &Path) -> Result<MountedFilesystem> {
        #[cfg(target_os = "linux")]
        {
            linux::observe(root)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = root;
            bail!("strict storage readiness has no host identity adapter on this platform");
        }
    }
}

pub(crate) fn verify(settings: &Settings) -> Result<()> {
    verify_with(settings, &HostIdentityProvider)
}

pub(super) fn verify_with(settings: &Settings, provider: &dyn IdentityProvider) -> Result<()> {
    let Some(config) = &settings.storage.readiness else {
        return Ok(());
    };
    if !config.strict {
        return Ok(());
    }
    let roots = config.roots(settings)?;
    let mut verified = Vec::new();
    for root in &roots {
        let path = root.path.canonicalize().with_context(|| {
            format!(
                "readiness {} directory must already exist: {}",
                root.role,
                root.path.display()
            )
        })?;
        if !path.is_dir() {
            bail!("readiness {} root is not a directory", root.role)
        }
        let mountpoint = &root.filesystem.mountpoint;
        // A declared mount root must itself be canonical, not a symlink alias.
        absolute_path(mountpoint)?;
        if mountpoint.canonicalize()? != *mountpoint {
            bail!(
                "readiness mountpoint must be canonical: {}",
                mountpoint.display()
            );
        }
        let facts = provider
            .observe(&path)
            .with_context(|| format!("storage readiness failed for {}", root.role))?;
        if facts.mountpoint != *mountpoint
            || facts.filesystem_root != Path::new("/")
            || !path.starts_with(mountpoint)
        {
            bail!(
                "readiness {} is not on the declared mounted filesystem root",
                root.role
            );
        }
        if facts.identity != root.filesystem.identity {
            bail!(
                "readiness {} filesystem identity changed or does not match the declaration",
                root.role
            );
        }
        if !facts.writable || !facts.unlocked {
            bail!(
                "readiness {} filesystem must be writable, mounted and unlocked",
                root.role
            );
        }
        verified.push((root, path));
    }
    for (index, (left, left_path)) in verified.iter().enumerate() {
        for (right, right_path) in &verified[index + 1..] {
            if left_path.starts_with(right_path) || right_path.starts_with(left_path) {
                bail!(
                    "readiness roots must not overlap: {} and {}",
                    left.role,
                    right.role
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
