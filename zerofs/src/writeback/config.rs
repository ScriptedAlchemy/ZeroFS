use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckMode {
    Remote,
    Ssd,
    // Memory acknowledgement is the tier's default contract: bursts land at
    // RAM speed and stay volatile until they cross the SSD journal — exactly
    // like an OS page cache — while client flush barriers (fsync/FUA/COMMIT
    // route through `wait_local_through_accepted`) still force SSD
    // durability before they return.
    #[default]
    Memory,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShutdownFlush {
    #[default]
    Local,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritebackAccessMode {
    ReadWrite,
    ReadOnly,
    Checkpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritebackConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(
        default,
        deserialize_with = "crate::config::deserialize_expandable_path"
    )]
    pub(crate) dir: PathBuf,
    #[serde(default)]
    pub(crate) ack_mode: AckMode,
    #[serde(default)]
    pub(crate) memory_size_gb: f64,
    #[serde(default)]
    pub(crate) disk_size_gb: f64,
    #[serde(default)]
    pub(crate) min_free_gb: f64,
    #[serde(default = "default_high_watermark_percent")]
    pub(crate) high_watermark_percent: u8,
    #[serde(default = "default_resume_percent")]
    pub(crate) resume_percent: u8,
    /// Concurrent remote uploads. Defaults to four for generic backends. SFTP
    /// defaults to seven, clamped to `[sftp] write_concurrency`, so its default
    /// eight-session pool retains one lane for reads and control traffic.
    #[serde(default)]
    pub(crate) upload_concurrency: Option<usize>,
    #[serde(default = "default_local_concurrency")]
    pub(crate) local_concurrency: usize,
    #[serde(default)]
    pub(crate) shutdown_flush: ShutdownFlush,
}

impl Default for WritebackConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: PathBuf::new(),
            ack_mode: AckMode::Ssd,
            memory_size_gb: 0.0,
            disk_size_gb: 0.0,
            min_free_gb: 0.0,
            high_watermark_percent: default_high_watermark_percent(),
            resume_percent: default_resume_percent(),
            upload_concurrency: None,
            local_concurrency: default_local_concurrency(),
            shutdown_flush: ShutdownFlush::Local,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritebackSettings {
    pub(crate) dir: PathBuf,
    pub(crate) ack_mode: AckMode,
    pub(crate) memory_bytes: u64,
    pub(crate) disk_bytes: u64,
    pub(crate) min_free_bytes: u64,
    pub(crate) high_watermark_percent: u8,
    pub(crate) resume_percent: u8,
    pub(crate) upload_concurrency: usize,
    pub(crate) local_concurrency: usize,
    pub(crate) shutdown_flush: ShutdownFlush,
}

impl WritebackConfig {
    pub(crate) fn normalize(
        &self,
        clean_cache_dir: &Path,
        sftp_write_concurrency: Option<usize>,
        access_mode: WritebackAccessMode,
        replication_enabled: bool,
    ) -> Result<Option<WritebackSettings>> {
        if !self.enabled {
            return Ok(None);
        }
        if access_mode != WritebackAccessMode::ReadWrite {
            bail!("[writeback] requires a read-write server");
        }
        if replication_enabled {
            bail!("[writeback] is not supported with [replication] read-write mode");
        }

        let dir = normalize_absolute_path(&self.dir, "[writeback] dir")?;
        let clean_cache_dir = normalize_absolute_path(clean_cache_dir, "[cache] dir")?;
        if dir == clean_cache_dir || dir.starts_with(&clean_cache_dir) {
            bail!("[writeback] dir must not be inside the clean cache directory");
        }

        let memory_bytes = decimal_gb_to_bytes("memory_size_gb", self.memory_size_gb)?;
        let disk_bytes = decimal_gb_to_bytes("disk_size_gb", self.disk_size_gb)?;
        let min_free_bytes = decimal_gb_to_bytes("min_free_gb", self.min_free_gb)?;

        if memory_bytes == 0 {
            bail!("[writeback] memory_size_gb must be greater than zero when writeback is enabled");
        }
        if disk_bytes == 0 {
            bail!(
                "[writeback] disk_size_gb must be greater than zero; every acknowledgement mode uses the independent SSD journal"
            );
        }
        if disk_bytes > 0 && min_free_bytes == 0 {
            bail!(
                "[writeback] min_free_gb must be greater than zero when SSD journaling is enabled"
            );
        }
        if !(0 < self.resume_percent
            && self.resume_percent < self.high_watermark_percent
            && self.high_watermark_percent <= 100)
        {
            bail!("[writeback] requires 0 < resume_percent < high_watermark_percent <= 100");
        }
        if !(1..=256).contains(&self.local_concurrency) {
            bail!("[writeback] local_concurrency must be between 1 and 256");
        }
        let upload_concurrency = match self.upload_concurrency {
            Some(0) => bail!("[writeback] upload_concurrency must be greater than zero"),
            Some(explicit) => {
                if let Some(limit) = sftp_write_concurrency
                    && explicit > limit
                {
                    bail!(
                        "[writeback] upload_concurrency ({explicit}) must not exceed [sftp] write_concurrency ({limit})"
                    );
                }
                explicit
            }
            // SFTP has its own stream-aware default. Other backends retain the
            // conservative generic default instead of inheriting SFTP tuning.
            None => match sftp_write_concurrency {
                Some(limit) => default_sftp_upload_concurrency().min(limit).max(1),
                None => default_upload_concurrency(),
            },
        };

        Ok(Some(WritebackSettings {
            dir,
            ack_mode: self.ack_mode,
            memory_bytes,
            disk_bytes,
            min_free_bytes,
            high_watermark_percent: self.high_watermark_percent,
            resume_percent: self.resume_percent,
            upload_concurrency,
            local_concurrency: self.local_concurrency,
            shutdown_flush: self.shutdown_flush,
        }))
    }
}

const fn default_high_watermark_percent() -> u8 {
    95
}

const fn default_resume_percent() -> u8 {
    85
}

// Conservative default shared by non-SFTP backends.
const fn default_upload_concurrency() -> usize {
    4
}

// Publication lanes multiplex onto pooled SFTP connections, so more lanes
// than connections hide the fixed per-object round trips. Eight lanes fill
// half the default write-operation budget, leaving room for multipart parts
// and finalization, and bound worst-case in-flight record memory.
const fn default_sftp_upload_concurrency() -> usize {
    8
}

const fn default_local_concurrency() -> usize {
    4
}

fn decimal_gb_to_bytes(name: &str, value: f64) -> Result<u64> {
    if !value.is_finite() || value < 0.0 {
        bail!("[writeback] {name} must be a finite non-negative number");
    }
    let bytes = value * 1_000_000_000.0;
    if bytes > u64::MAX as f64 {
        bail!("[writeback] {name} is too large");
    }
    Ok(bytes.round() as u64)
}

fn normalize_absolute_path(path: &Path, name: &str) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("{name} must be configured when writeback is enabled");
    }
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => bail!("{name} must not contain '..' components"),
            Component::Normal(part) => normalized.push(part),
        }
    }
    let mut existing = normalized.as_path();
    let mut missing = Vec::new();
    loop {
        match existing.symlink_metadata() {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = existing
                    .file_name()
                    .with_context(|| format!("failed to find an existing ancestor for {name}"))?;
                missing.push(component.to_os_string());
                existing = existing
                    .parent()
                    .with_context(|| format!("failed to find an existing ancestor for {name}"))?;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to normalize {name}"));
            }
        }
    }

    let mut resolved = existing
        .canonicalize()
        .with_context(|| format!("failed to normalize {name}"))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_writeback_at(dir: PathBuf) -> WritebackConfig {
        WritebackConfig {
            dir,
            ..enabled_writeback()
        }
    }

    fn enabled_writeback() -> WritebackConfig {
        WritebackConfig {
            enabled: true,
            dir: std::env::temp_dir().join("zerofs-writeback-default-test"),
            memory_size_gb: 1.0,
            disk_size_gb: 1.0,
            min_free_gb: 1.0,
            ..WritebackConfig::default()
        }
    }

    #[test]
    fn default_upload_lanes_are_backend_aware() {
        let config = enabled_writeback();
        let clean_cache = std::env::temp_dir().join("zerofs-clean-cache-default-test");

        let generic = config
            .normalize(&clean_cache, None, WritebackAccessMode::ReadWrite, false)
            .unwrap()
            .unwrap();
        let sftp = config
            .normalize(&clean_cache, Some(8), WritebackAccessMode::ReadWrite, false)
            .unwrap()
            .unwrap();

        assert_eq!(generic.upload_concurrency, 4);
        assert_eq!(sftp.upload_concurrency, 8);
    }

    #[cfg(unix)]
    #[test]
    fn missing_writeback_leaf_under_cache_parent_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let clean = root.path().join("clean");
        std::fs::create_dir(&clean).unwrap();
        let alias = root.path().join("alias");
        symlink(&clean, &alias).unwrap();

        let error = enabled_writeback_at(alias.join("new-dirty"))
            .normalize(&clean, None, WritebackAccessMode::ReadWrite, false)
            .unwrap_err();

        assert!(
            error.to_string().contains("inside the clean cache"),
            "{error:#}"
        );
        assert!(!clean.join("new-dirty").exists());
    }

    #[test]
    fn prospective_normalization_preserves_nested_missing_suffix_without_creating_it() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("first").join("nested").join("dirty");

        let normalized = normalize_absolute_path(&path, "test path").unwrap();

        assert_eq!(
            normalized,
            root.path()
                .canonicalize()
                .unwrap()
                .join("first/nested/dirty")
        );
        assert!(!root.path().join("first").exists());
    }

    #[test]
    fn missing_sibling_cache2_path_remains_valid() {
        let root = tempfile::tempdir().unwrap();
        let clean = root.path().join("cache");
        std::fs::create_dir(&clean).unwrap();
        let dirty = root.path().join("cache2").join("new-dirty");

        let settings = enabled_writeback_at(dirty.clone())
            .normalize(&clean, None, WritebackAccessMode::ReadWrite, false)
            .unwrap()
            .unwrap();

        assert_eq!(
            settings.dir,
            root.path().canonicalize().unwrap().join("cache2/new-dirty")
        );
        assert!(!dirty.exists());
    }

    #[test]
    fn separate_missing_first_start_paths_remain_valid() {
        let root = tempfile::tempdir().unwrap();
        let clean = root.path().join("clean");
        let dirty = root.path().join("dirty");

        let settings = enabled_writeback_at(dirty.clone())
            .normalize(&clean, None, WritebackAccessMode::ReadWrite, false)
            .unwrap()
            .unwrap();

        assert_eq!(
            settings.dir,
            root.path().canonicalize().unwrap().join("dirty")
        );
        assert!(!clean.exists());
        assert!(!dirty.exists());
    }

    #[test]
    fn direct_nested_missing_writeback_path_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let clean = root.path().join("clean");

        let error = enabled_writeback_at(clean.join("nested/dirty"))
            .normalize(&clean, None, WritebackAccessMode::ReadWrite, false)
            .unwrap_err();

        assert!(
            error.to_string().contains("inside the clean cache"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn dangling_parent_symlink_is_an_error() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let dangling = root.path().join("dangling");
        symlink(root.path().join("missing-target"), &dangling).unwrap();

        let error = normalize_absolute_path(&dangling.join("dirty"), "test path").unwrap_err();

        assert!(
            error.to_string().contains("failed to normalize test path"),
            "{error:#}"
        );
    }

    #[test]
    fn non_directory_existing_ancestor_preserves_the_filesystem_error() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("not-a-directory");
        std::fs::write(&file, b"file").unwrap();

        let error = normalize_absolute_path(&file.join("dirty"), "test path").unwrap_err();

        assert!(
            error.to_string().contains("failed to normalize test path"),
            "{error:#}"
        );
    }

    #[test]
    fn relative_and_parent_components_remain_rejected() {
        let relative =
            normalize_absolute_path(Path::new("relative/dirty"), "test path").unwrap_err();
        assert!(relative.to_string().contains("must be an absolute path"));

        let root = tempfile::tempdir().unwrap();
        let parent =
            normalize_absolute_path(&root.path().join("../dirty"), "test path").unwrap_err();
        assert!(parent.to_string().contains("must not contain '..'"));
    }
}
