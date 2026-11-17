mod config;
mod devices;
mod identity;
#[cfg(any(target_os = "linux", test))]
mod linux;
pub(crate) mod paths;

use crate::config::Settings;
use anyhow::{Context, Result, bail};
#[cfg(any(target_os = "linux", test))]
use config::FilesystemIdentity;
pub(crate) use config::ReadinessConfig;
use config::StorageRoot;
pub(crate) use devices::RoleName;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use identity::{MountedFilesystem, Serial};

pub(super) trait IdentityProvider {
    fn observe(&self, root: &Path) -> Result<MountedFilesystem>;

    fn filesystem_size(&self, root: &Path) -> Result<u64>;

    fn observe_device(&self, _by_id: &Path) -> Result<Serial> {
        bail!("device identity unavailable from this adapter")
    }

    fn backing_serials(&self, _root: &Path) -> Result<HashSet<Serial>> {
        bail!("backing device serials unavailable from this adapter")
    }
}

struct HostIdentityProvider;

impl IdentityProvider for HostIdentityProvider {
    fn filesystem_size(&self, root: &Path) -> Result<u64> {
        let stat = rustix::fs::statvfs(root).context("statvfs filesystem size unavailable")?;
        checked_filesystem_size(stat.f_blocks, stat.f_frsize)
    }

    #[cfg(target_os = "linux")]
    fn observe_device(&self, by_id: &Path) -> Result<Serial> {
        linux::devices::observe_device(by_id)
    }

    #[cfg(target_os = "linux")]
    fn backing_serials(&self, root: &Path) -> Result<HashSet<Serial>> {
        linux::devices::backing_serials(root)
    }

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

fn checked_filesystem_size(blocks: u64, fragment_size: u64) -> Result<u64> {
    blocks
        .checked_mul(fragment_size)
        .filter(|size| *size > 0)
        .context("statvfs filesystem size unavailable or overflowing")
}

pub(crate) fn verify(settings: &Settings) -> Result<()> {
    verify_with(settings, &HostIdentityProvider)
}

pub(super) fn verify_with(settings: &Settings, provider: &dyn IdentityProvider) -> Result<()> {
    let Some(config) = &settings.storage.readiness else {
        return Ok(());
    };
    let roots = config.validated_roots(settings)?;
    if !config.strict {
        for root in &roots {
            paths::verify_role(root)
                .with_context(|| format!("storage readiness failed for {}", root.role))?;
        }
        return Ok(());
    }
    for device in &config.devices {
        let serial = provider.observe_device(&device.by_id).with_context(|| {
            format!(
                "storage readiness device evidence unavailable at {}",
                device.by_id.display()
            )
        })?;
        if serial != device.serial {
            bail!(
                "readiness device serial does not match its declaration at {}",
                device.by_id.display()
            );
        }
    }
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
        verify_identity(config, root, &path, provider)?;
        paths::verify_role(root)
            .with_context(|| format!("storage readiness failed for {}", root.role))?;
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
    config: &ReadinessConfig,
    root: &StorageRoot<'_>,
    path: &Path,
    provider: &dyn IdentityProvider,
) -> Result<()> {
    let mountpoint = &root.filesystem.mountpoint;
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
    let size = provider
        .filesystem_size(path)
        .with_context(|| format!("readiness {} filesystem size unavailable", root.role))?;
    if root.reserve_bytes > size {
        bail!(
            "readiness {} reserve exceeds observed filesystem size",
            root.role
        );
    }
    if !config.devices.is_empty() {
        let serials = provider.backing_serials(path).with_context(|| {
            format!(
                "readiness {} backing device evidence unavailable",
                root.role
            )
        })?;
        if config
            .devices
            .iter()
            .any(|device| device.boot && serials.contains(&device.serial))
        {
            bail!(
                "readiness {} is backed by a declared boot device",
                root.role
            );
        }
        let declared = config
            .devices
            .iter()
            .filter(|device| device.roles.contains(&root.role))
            .map(|device| device.serial.clone())
            .collect::<HashSet<_>>();
        if serials.is_empty() || serials != declared {
            bail!(
                "readiness {} backing device set does not match declared roles",
                root.role
            );
        }
    }
    Ok(())
}

pub(crate) fn verify_open_paths(settings: &Settings, paths: &[(RoleName, PathBuf)]) -> Result<()> {
    verify_open_paths_with(settings, &HostIdentityProvider, paths)
}

fn verify_open_paths_with(
    settings: &Settings,
    provider: &dyn IdentityProvider,
    paths: &[(RoleName, PathBuf)],
) -> Result<()> {
    let Some(config) = &settings.storage.readiness else {
        return Ok(());
    };
    let roots = config.validated_roots(settings)?;
    for (role, path) in paths {
        let root = roots
            .iter()
            .find(|root| &root.role == role)
            .with_context(|| format!("undeclared readiness open role {role}"))?;
        if !path.starts_with(&root.path) {
            bail!("readiness {role} open path escapes its declared role");
        }
        let existing = paths::existing_ancestor(&root.path, path)?;
        if !config.strict {
            continue;
        }
        let existing = existing.canonicalize()?;
        if !existing.starts_with(root.path.canonicalize()?) {
            bail!("readiness {role} open path escapes its declared role");
        }
        verify_identity(config, root, &existing, provider)?;
    }
    Ok(())
}

pub(crate) fn verify_pool_paths(settings: &Settings, pool_id: &str) -> Result<()> {
    let Some(config) = &settings.storage.readiness else {
        return Ok(());
    };
    let roots = config.validated_roots(settings)?;
    for root in &roots {
        let RoleName::Member(id) = &root.role else {
            continue;
        };
        let pool = root.path.join(".zerofs-pool").join(pool_id);
        let lock = crate::local_object_store::LOCK_FILE;
        for path in [
            pool.join("members").join(id),
            pool.join("members").join(lock),
            pool.join("objects"),
            pool.join("objects").join(lock),
        ] {
            paths::existing_ancestor(&root.path, &path)?;
        }
    }
    Ok(())
}

pub(crate) fn verify_bucket_paths(settings: &Settings, namespace: &str) -> Result<()> {
    let cache = settings.cache.dir.join(namespace);
    let mut paths = vec![
        (RoleName::Cache, cache.clone()),
        (RoleName::Cache, cache.join(".zerofs-clean-cache.lock")),
        (RoleName::Cache, cache.join("hybrid_cache")),
        (
            RoleName::Cache,
            cache.join(crate::object_store_prefetch::LEGACY_PARTS_CACHE_DIR),
        ),
        (
            RoleName::Cache,
            cache.join(crate::object_store_prefetch::PARTS_CACHE_DIR),
        ),
        (
            RoleName::Cache,
            cache
                .join(crate::object_store_prefetch::PARTS_CACHE_DIR)
                .join(crate::object_store_prefetch::PARTS_CACHE_LAYOUT_FILE),
        ),
    ];
    if let Some(writeback) = settings
        .writeback
        .as_ref()
        .filter(|writeback| writeback.enabled)
    {
        let journal = writeback.dir.join(namespace);
        paths.extend([
            (RoleName::Journal, journal.clone()),
            (RoleName::Journal, journal.join("LOCK")),
            (RoleName::Journal, journal.join("journal.redb")),
            (RoleName::Journal, journal.join("blobs")),
            (RoleName::Journal, journal.join("tmp")),
            (RoleName::Journal, journal.join("tmp/multipart")),
        ]);
    }
    verify_open_paths(settings, &paths)?;
    if settings.storage.readiness.is_some() {
        paths::verify_cache_partitions(&cache)?;
        if let Some(writeback) = settings
            .writeback
            .as_ref()
            .filter(|writeback| writeback.enabled)
        {
            // These 256 shard directories are fixed by the journal format.
            // Check their components without enumerating any blob entries.
            for shard in 0..256 {
                paths::existing_ancestor(
                    &writeback.dir,
                    &writeback
                        .dir
                        .join(namespace)
                        .join(format!("blobs/{shard:02x}")),
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
