mod config;
mod devices;
mod identity;
#[cfg(any(target_os = "linux", test))]
mod linux;

use crate::config::Settings;
use anyhow::{Context, Result, bail};
#[cfg(any(target_os = "linux", test))]
use config::FilesystemIdentity;
pub(crate) use config::ReadinessConfig;
use config::{StorageRoot, absolute_path};
use devices::RoleName;
use std::path::{Path, PathBuf};

use identity::MountedFilesystem;

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
    let roots = config.validated_roots(settings)?;
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
        verify_identity(root, &path, provider)?;
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

fn verify_identity(
    root: &StorageRoot<'_>,
    path: &Path,
    provider: &dyn IdentityProvider,
) -> Result<()> {
    let mountpoint = &root.filesystem.mountpoint;
    absolute_path(mountpoint)?;
    if mountpoint.canonicalize()? != *mountpoint {
        bail!(
            "readiness mountpoint must be canonical: {}",
            mountpoint.display()
        );
    }
    let facts = provider
        .observe(path)
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
    Ok(())
}

pub(crate) fn verify_open_paths(settings: &Settings, paths: &[(&str, PathBuf)]) -> Result<()> {
    verify_open_paths_with(settings, &HostIdentityProvider, paths)
}

fn verify_open_paths_with(
    settings: &Settings,
    provider: &dyn IdentityProvider,
    paths: &[(&str, PathBuf)],
) -> Result<()> {
    verify_with(settings, provider)?;
    let Some(config) = settings
        .storage
        .readiness
        .as_ref()
        .filter(|config| config.strict)
    else {
        return Ok(());
    };
    let roots = config.validated_roots(settings)?;
    for (role, path) in paths {
        absolute_path(path)?;
        let role_name = RoleName::try_from((*role).to_owned()).map_err(anyhow::Error::msg)?;
        let root = roots
            .iter()
            .find(|root| root.role == role_name)
            .with_context(|| format!("undeclared readiness open role {role}"))?;
        if !path.starts_with(&root.path) {
            bail!("readiness {role} open path escapes its declared role");
        }
        let mut existing = path.as_path();
        loop {
            match existing.symlink_metadata() {
                Ok(_) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    existing = existing
                        .parent()
                        .context("readiness open path has no existing ancestor")?;
                }
                Err(error) => return Err(error).context("cannot inspect readiness open path"),
            }
        }
        let existing = existing.canonicalize()?;
        if !existing.starts_with(root.path.canonicalize()?) {
            bail!("readiness {role} open path escapes its declared role");
        }
        verify_identity(root, &existing, provider)?;
    }
    Ok(())
}

pub(crate) fn verify_bucket_paths(settings: &Settings, namespace: &str) -> Result<()> {
    let cache = settings.cache.dir.join(namespace);
    let mut paths = vec![
        ("cache", cache.clone()),
        ("cache", cache.join(".zerofs-clean-cache.lock")),
        ("cache", cache.join("hybrid_cache")),
        ("cache", cache.join("parts_cache")),
    ];
    if let Some(writeback) = settings
        .writeback
        .as_ref()
        .filter(|writeback| writeback.enabled)
    {
        let journal = writeback.dir.join(namespace);
        paths.extend([
            ("journal", journal.clone()),
            ("journal", journal.join("LOCK")),
            ("journal", journal.join("journal.redb")),
            ("journal", journal.join("blobs")),
            ("journal", journal.join("tmp")),
        ]);
    }
    verify_open_paths(settings, &paths)
}

#[cfg(test)]
mod tests;
