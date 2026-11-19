//! Restart-only override for the three filesystem write acknowledgement settings.
use super::Settings;
use crate::fs::mutation::config::FilesystemWriteAckMode;
#[cfg(feature = "webui")]
use crate::fs::mutation::config::FilesystemWriteAckSettings;
use crate::writeback::config::normalize_absolute_path;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) const FILE_ENV: &str = "ZEROFS_WRITE_ACK_SETTINGS_FILE";
pub(crate) const MAX_DOCUMENT: usize = 2048;
const MIB: u64 = 1 << 20;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "write_ack_mode", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Fields {
    Materialized {},
    VolatileMemory {
        volatile_memory_gb: f64,
        volatile_max_operations: usize,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Document {
    pub(crate) version: u32,
    pub(crate) filesystem: Fields,
}

pub(crate) fn bounded_ram(gb: f64, operations: usize) -> bool {
    let mib = (gb * 1e9 / MIB as f64).round() as u64;
    (16..=256).contains(&mib) && gb == (mib * MIB) as f64 / 1e9 && (1..=2048).contains(&operations)
}

impl Fields {
    #[cfg(feature = "webui")]
    pub(crate) fn effective(ack: FilesystemWriteAckSettings) -> Self {
        match ack.mode {
            FilesystemWriteAckMode::Materialized => Self::Materialized {},
            FilesystemWriteAckMode::VolatileMemory => Self::VolatileMemory {
                volatile_memory_gb: ack.volatile_memory_bytes as f64 / 1e9,
                volatile_max_operations: ack.volatile_max_operations,
            },
        }
    }
}

impl Document {
    pub(crate) fn apply(&self, settings: &mut Settings) -> Result<()> {
        ensure!(self.version == 1, "unknown write-ack document version");
        ensure!(
            settings.small_nas_journal(),
            "write-ack override requires small-nas-journal-v1 or small-nas-journal-v2"
        );
        let fs = settings
            .filesystem
            .as_mut()
            .context("missing [filesystem]")?;
        let (mode, gb, ops) = match self.filesystem {
            Fields::Materialized {} => (FilesystemWriteAckMode::Materialized, 0., None),
            Fields::VolatileMemory {
                volatile_memory_gb,
                volatile_max_operations,
            } => {
                ensure!(
                    bounded_ram(volatile_memory_gb, volatile_max_operations),
                    "volatile budget requires 16–256 whole MiB and 1–2048 operations"
                );
                (
                    FilesystemWriteAckMode::VolatileMemory,
                    volatile_memory_gb,
                    Some(volatile_max_operations),
                )
            }
        };
        fs.write_ack_mode = Some(mode);
        fs.volatile_memory_gb = gb;
        fs.volatile_max_operations = ops;
        settings.validate()
    }
}

pub(crate) fn check_path(settings: &Settings, path: &Path, toml: &Path) -> Result<()> {
    ensure!(
        settings.small_nas_journal(),
        "write-ack settings require small-nas-journal-v1 or small-nas-journal-v2"
    );
    ensure!(
        path.is_absolute() && path.file_name().is_some(),
        "expected absolute private file path"
    );
    let parent = path.parent().context("missing private directory")?;
    let actual = parent
        .canonicalize()
        .context("private settings directory unavailable")?;
    ensure!(
        actual == parent && path != toml.canonicalize()?,
        "private path aliases a link or the engine configuration"
    );
    let meta = actual.metadata()?;
    ensure!(
        meta.is_dir() && meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0,
        "settings directory must be owned by the engine user and private"
    );
    // Protect pathname-based atomic replacement from an untrusted user
    // renaming a directory above the private parent. A system sticky /tmp
    // still protects this user's owned child.
    let uid = unsafe { libc::geteuid() };
    for dir in actual.ancestors().skip(1) {
        let ancestor = dir.metadata()?;
        ensure!(
            (ancestor.uid() == uid || ancestor.uid() == 0)
                && (ancestor.mode() & 0o022 == 0 || ancestor.mode() & 0o1000 != 0),
            "private settings directory has an untrusted ancestor"
        );
    }
    #[cfg(target_os = "linux")]
    {
        let mount = rustix::fs::fstatfs(std::fs::File::open(&actual)?)?;
        ensure!(
            mount.f_type != libc::FUSE_SUPER_MAGIC && mount.f_type != 0x01021997,
            "private settings cannot be placed on a FUSE or 9P file export"
        );
    }
    let pool = settings
        .storage
        .pool
        .as_ref()
        .context("missing local pool")?;
    let mut roots = vec![settings.cache.dir.clone(), pool.catalog_dir.clone()];
    if let Some(wb) = &settings.writeback {
        roots.push(wb.dir.clone());
    }
    for url in std::iter::once(&settings.storage.url).chain(pool.members.iter().map(|m| &m.url)) {
        roots.push(
            url::Url::parse(url)?
                .to_file_path()
                .map_err(|_| anyhow::anyhow!("expected a local storage directory"))?,
        );
    }
    for root in roots {
        let root = normalize_absolute_path(&root, "storage root")?;
        ensure!(
            !path.starts_with(&root) && !root.starts_with(path),
            "private settings path overlaps an engine storage root"
        );
    }
    Ok(())
}

pub(crate) fn private_bytes(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let meta = file.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0,
        "expected a private regular file owned by the engine user"
    );
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "private file exceeds its size limit");
    Ok(Some(bytes))
}

pub(crate) fn read_document(path: &Path) -> Result<Option<Document>> {
    private_bytes(path, MAX_DOCUMENT)?
        .map(|bytes| serde_json::from_slice(&bytes).context("invalid write-ack settings document"))
        .transpose()
}

#[cfg(feature = "webui")]
pub(crate) fn persist(path: &Path, doc: &Document) -> Result<()> {
    use std::io::Write;
    let parent = path.parent().context("missing private directory")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, doc)?;
    temp.write_all(b"\n")?;
    temp.as_file().sync_all()?;
    temp.persist(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

impl Settings {
    pub(crate) fn bounded_journal_ram(&self) -> bool {
        self.small_nas_journal()
            && self.filesystem.as_ref().is_some_and(|f| {
                f.write_ack_mode == Some(FilesystemWriteAckMode::VolatileMemory)
                    && f.volatile_max_operations
                        .is_some_and(|ops| bounded_ram(f.volatile_memory_gb, ops))
            })
    }

    pub(crate) fn from_runtime_file(config: &Path) -> Result<Self> {
        let path = std::env::var_os(FILE_ENV).map(PathBuf::from);
        Self::from_runtime_file_with_override(config, path.as_deref())
    }

    pub(crate) fn from_runtime_file_with_override(
        config: &Path,
        path: Option<&Path>,
    ) -> Result<Self> {
        let mut settings = Settings::from_file(config)?;
        if let Some(path) = path {
            check_path(&settings, path, config)?;
            if let Some(doc) = read_document(path)? {
                doc.apply(&mut settings)?;
            }
        }
        Ok(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn private_reader_rejects_public_linked_and_oversized_inputs() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("settings");
        assert!(private_bytes(&path, 16).unwrap().is_none());
        std::fs::write(&path, b"data").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(private_bytes(&path, 16).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(private_bytes(&path, 3).is_err());
        let link = root.path().join("alias");
        symlink(&path, &link).unwrap();
        assert!(private_bytes(&link, 16).is_err());
        std::fs::remove_file(&link).unwrap();
        std::fs::hard_link(&path, &link).unwrap();
        assert!(private_bytes(&path, 16).is_err());
    }
}
