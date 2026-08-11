use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckMode {
    Remote,
    #[default]
    Ssd,
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
    pub enabled: bool,
    #[serde(
        default,
        deserialize_with = "crate::config::deserialize_expandable_path"
    )]
    pub dir: PathBuf,
    #[serde(default)]
    pub ack_mode: AckMode,
    #[serde(default)]
    pub memory_size_gb: f64,
    #[serde(default)]
    pub disk_size_gb: f64,
    #[serde(default)]
    pub min_free_gb: f64,
    #[serde(default = "default_high_watermark_percent")]
    pub high_watermark_percent: u8,
    #[serde(default = "default_resume_percent")]
    pub resume_percent: u8,
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
    #[serde(default)]
    pub shutdown_flush: ShutdownFlush,
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
            upload_concurrency: default_upload_concurrency(),
            shutdown_flush: ShutdownFlush::Local,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritebackSettings {
    pub dir: PathBuf,
    pub ack_mode: AckMode,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub min_free_bytes: u64,
    pub high_watermark_percent: u8,
    pub resume_percent: u8,
    pub upload_concurrency: usize,
    pub shutdown_flush: ShutdownFlush,
}

impl WritebackConfig {
    pub fn normalize(
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
        if matches!(self.ack_mode, AckMode::Memory | AckMode::Ssd) && disk_bytes == 0 {
            bail!("[writeback] disk_size_gb must be greater than zero in memory or ssd mode");
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
        if self.upload_concurrency == 0 {
            bail!("[writeback] upload_concurrency must be greater than zero");
        }
        if let Some(limit) = sftp_write_concurrency
            && self.upload_concurrency > limit
        {
            bail!(
                "[writeback] upload_concurrency ({}) must not exceed [sftp] write_concurrency ({limit})",
                self.upload_concurrency
            );
        }

        Ok(Some(WritebackSettings {
            dir,
            ack_mode: self.ack_mode,
            memory_bytes,
            disk_bytes,
            min_free_bytes,
            high_watermark_percent: self.high_watermark_percent,
            resume_percent: self.resume_percent,
            upload_concurrency: self.upload_concurrency,
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

const fn default_upload_concurrency() -> usize {
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
    normalized
        .canonicalize()
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(normalized.clone())
            } else {
                Err(error)
            }
        })
        .with_context(|| format!("failed to normalize {name}"))
}
