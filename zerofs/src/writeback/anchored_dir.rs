//! Descriptor-anchored access to the writeback journal tree.

use anyhow::{Context, Result, bail};
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, mkdirat, open, openat, statat, unlinkat};
use rustix::io::Errno;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Clone, Debug)]
pub(crate) struct AnchoredDir {
    fd: Arc<OwnedFd>,
    display: PathBuf,
}

impl AnchoredDir {
    pub(crate) fn open_absolute(path: &Path, expected_mode: u32) -> Result<Self> {
        Self::walk_absolute(path, false, expected_mode)
    }

    pub(crate) fn open_or_create_absolute(path: &Path, mode: u32) -> Result<Self> {
        Self::walk_absolute(path, true, mode)
    }

    fn walk_absolute(path: &Path, create: bool, mode: u32) -> Result<Self> {
        if !path.is_absolute() {
            bail!("anchored directory path must be absolute");
        }
        let mut current = Self {
            fd: Arc::new(
                open("/", DIRECTORY_FLAGS, Mode::empty())
                    .map_err(anyhow::Error::new)
                    .context("open writeback filesystem root")?,
            ),
            display: PathBuf::from("/"),
        };
        for component in path.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(name) => {
                    current = current
                        .open_child_raw(name, create, mode)?
                        .with_context(|| {
                            format!("anchored directory {} does not exist", path.display())
                        })?
                }
                Component::ParentDir | Component::Prefix(_) => {
                    bail!("anchored directory path contains an unsupported component")
                }
            }
        }
        current.validate_owner_mode(mode)?;
        Ok(current)
    }

    pub(crate) fn open_or_create_child(&self, name: &OsStr, mode: u32) -> Result<Self> {
        validate_component(name)?;
        let child = self
            .open_child_raw(name, true, mode)?
            .expect("create requested");
        child.converge_owned_mode(mode)?;
        child.validate_owner_mode(mode)?;
        Ok(child)
    }

    pub(crate) fn open_child(&self, name: &OsStr, expected_mode: u32) -> Result<Self> {
        validate_component(name)?;
        let child = self
            .open_child_raw(name, false, expected_mode)?
            .with_context(|| {
                format!(
                    "anchored directory {} does not exist",
                    self.display.join(name).display()
                )
            })?;
        child.validate_owner_mode(expected_mode)?;
        Ok(child)
    }

    fn open_child_raw(&self, name: &OsStr, create: bool, mode: u32) -> Result<Option<Self>> {
        let (fd, created) = match openat(self.fd.as_ref(), name, DIRECTORY_FLAGS, Mode::empty()) {
            Ok(fd) => (fd, false),
            Err(Errno::NOENT) if !create => return Ok(None),
            Err(Errno::NOENT) => {
                let created = match mkdirat(self.fd.as_ref(), name, rustix_mode(mode)?) {
                    Ok(()) => true,
                    Err(Errno::EXIST) => false,
                    Err(error) => {
                        return Err(anyhow::Error::new(error)).context("create anchored directory");
                    }
                };
                (
                    openat(self.fd.as_ref(), name, DIRECTORY_FLAGS, Mode::empty())
                        .map_err(anyhow::Error::new)
                        .context("open newly-created anchored directory")?,
                    created,
                )
            }
            Err(Errno::LOOP | Errno::NOTDIR) => {
                bail!("anchored directory contains a symlink or non-directory")
            }
            Err(error) => return Err(anyhow::Error::new(error)).context("open anchored directory"),
        };
        let child = Self {
            fd: Arc::new(fd),
            display: self.display.join(name),
        };
        if created {
            child.set_mode(mode)?;
            self.sync()?;
        }
        Ok(Some(child))
    }

    pub(crate) fn resolve_parent(
        &self,
        relative: &Path,
        create: bool,
        directory_mode: u32,
    ) -> Result<(Self, OsString)> {
        if relative.is_absolute() {
            bail!("anchored relative path must not be absolute");
        }
        let mut components = relative.components().peekable();
        let mut parent = self.clone();
        while let Some(component) = components.next() {
            let Component::Normal(name) = component else {
                bail!("anchored relative path contains an unsafe component");
            };
            if components.peek().is_none() {
                validate_component(name)?;
                return Ok((parent, name.to_os_string()));
            }
            parent = if create {
                parent.open_or_create_child(name, directory_mode)?
            } else {
                parent.open_child(name, directory_mode)?
            };
        }
        bail!("anchored relative path has no final component")
    }

    pub(crate) fn try_resolve_parent(
        &self,
        relative: &Path,
        directory_mode: u32,
    ) -> Result<Option<(Self, OsString)>> {
        if relative.is_absolute() {
            bail!("anchored relative path must not be absolute");
        }
        let mut components = relative.components().peekable();
        let mut parent = self.clone();
        while let Some(component) = components.next() {
            let Component::Normal(name) = component else {
                bail!("anchored relative path contains an unsafe component");
            };
            if components.peek().is_none() {
                validate_component(name)?;
                return Ok(Some((parent, name.to_os_string())));
            }
            let Some(child) = parent.open_child_raw(name, false, directory_mode)? else {
                return Ok(None);
            };
            child.validate_owner_mode(directory_mode)?;
            parent = child;
        }
        bail!("anchored relative path has no final component")
    }

    pub(crate) fn try_open_relative_dir(
        &self,
        relative: &Path,
        directory_mode: u32,
    ) -> Result<Option<Self>> {
        if relative.is_absolute() {
            bail!("anchored relative path must not be absolute");
        }
        let mut directory = self.clone();
        for component in relative.components() {
            let Component::Normal(name) = component else {
                bail!("anchored relative path contains an unsafe component");
            };
            let Some(child) = directory.open_child_raw(name, false, directory_mode)? else {
                return Ok(None);
            };
            child.validate_owner_mode(directory_mode)?;
            directory = child;
        }
        Ok(Some(directory))
    }

    pub(crate) fn open_file(&self, name: &OsStr, flags: OFlags, mode: u32) -> Result<File> {
        validate_component(name)?;
        let fd = openat(
            self.fd.as_ref(),
            name,
            flags
                .union(OFlags::NONBLOCK)
                .union(OFlags::NOFOLLOW)
                .union(OFlags::CLOEXEC),
            rustix_mode(mode)?,
        )
        .map_err(anyhow::Error::new)
        .with_context(|| format!("open anchored file {}", self.display.join(name).display()))?;
        Ok(File::from(fd))
    }

    pub(crate) fn open_owner_file(
        &self,
        name: &OsStr,
        allow_existing: bool,
    ) -> Result<(File, bool)> {
        self.open_owner_file_with(name, allow_existing, |_| Ok(()))
    }

    fn open_owner_file_with<F>(
        &self,
        name: &OsStr,
        allow_existing: bool,
        after_open: F,
    ) -> Result<(File, bool)>
    where
        F: FnOnce(&File) -> Result<()>,
    {
        validate_component(name)?;
        let existing_flags = OFlags::RDWR.union(OFlags::NOFOLLOW).union(OFlags::CLOEXEC);
        let (file, existed) = if allow_existing {
            match openat(self.fd.as_ref(), name, existing_flags, Mode::empty()) {
                Ok(fd) => (File::from(fd), true),
                Err(Errno::NOENT) => {
                    let fd = openat(
                        self.fd.as_ref(),
                        name,
                        existing_flags.union(OFlags::CREATE).union(OFlags::EXCL),
                        Mode::from_raw_mode(0o600),
                    )
                    .map_err(anyhow::Error::new)
                    .context("create anchored owner file")?;
                    (File::from(fd), false)
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error)).context("open anchored owner file");
                }
            }
        } else {
            let fd = openat(
                self.fd.as_ref(),
                name,
                existing_flags.union(OFlags::CREATE).union(OFlags::EXCL),
                Mode::from_raw_mode(0o600),
            )
            .map_err(anyhow::Error::new)
            .context("create anchored owner file")?;
            (File::from(fd), false)
        };
        let setup = (|| {
            if !existed {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            after_open(&file)?;
            self.validate_owner_file(name, &file)
        })();
        if let Err(error) = setup {
            if allow_existing || existed {
                return Err(error);
            }
            drop(file);
            return match self.remove_file_if_exists(name) {
                Ok(_) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "failed to remove exclusively-created anchored file after setup error: {cleanup:#}"
                ))),
            };
        }
        Ok((file, existed))
    }

    pub(crate) fn open_existing_owner_file_read_only(&self, name: &OsStr) -> Result<File> {
        validate_component(name)?;
        let fd = openat(
            self.fd.as_ref(),
            name,
            OFlags::RDONLY
                .union(OFlags::NONBLOCK)
                .union(OFlags::NOFOLLOW)
                .union(OFlags::CLOEXEC),
            Mode::empty(),
        )
        .map_err(anyhow::Error::new)
        .context("open anchored owner file read-only")?;
        let file = File::from(fd);
        self.validate_owner_file(name, &file)?;
        Ok(file)
    }

    fn validate_owner_file(&self, name: &OsStr, file: &File) -> Result<()> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            bail!(
                "journal file {} is not a regular file",
                self.display.join(name).display()
            );
        }
        crate::writeback::validate_owner_only(
            &self.display.join(name),
            &metadata,
            0o600,
            "journal file",
        )
    }

    pub(crate) fn remove_tree(&self, name: &OsStr) -> Result<()> {
        validate_component(name)?;
        let metadata = match statat(self.fd.as_ref(), name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(metadata) => metadata,
            Err(Errno::NOENT) => return Ok(()),
            Err(error) => {
                return Err(anyhow::Error::new(error)).context("inspect anchored entry");
            }
        };
        let file_type = FileType::from_raw_mode(metadata.st_mode);
        if file_type.is_dir() {
            let child = self
                .open_child_raw(name, false, 0)?
                .context("anchored directory disappeared during cleanup")?;
            child.for_each_entry(|entry| child.remove_tree(entry))?;
            unlinkat(self.fd.as_ref(), name, AtFlags::REMOVEDIR)
                .map_err(anyhow::Error::new)
                .context("remove anchored directory")
        } else if file_type.is_file() || file_type.is_symlink() {
            unlinkat(self.fd.as_ref(), name, AtFlags::empty())
                .map_err(anyhow::Error::new)
                .context("remove anchored file")
        } else {
            bail!("anchored cleanup entry is neither file, symlink, nor directory")
        }
    }

    pub(crate) fn remove_file(&self, name: &OsStr) -> Result<()> {
        self.remove_file_if_exists(name).map(drop)
    }

    pub(crate) fn remove_file_if_exists(&self, name: &OsStr) -> Result<bool> {
        validate_component(name)?;
        match unlinkat(self.fd.as_ref(), name, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(Errno::NOENT) => Ok(false),
            Err(error) => Err(anyhow::Error::new(error)).context("remove anchored file"),
        }
    }

    pub(crate) fn for_each_entry(&self, mut visit: impl FnMut(&OsStr) -> Result<()>) -> Result<()> {
        let scan = openat(self.fd.as_ref(), ".", DIRECTORY_FLAGS, Mode::empty())
            .map_err(anyhow::Error::new)
            .context("open independent anchored directory scan")?;
        let directory = Dir::new(scan)
            .map_err(anyhow::Error::new)
            .context("create anchored directory stream")?;
        for entry in directory {
            let entry = entry.map_err(anyhow::Error::new)?;
            let name = entry.file_name().to_bytes();
            if name != b"." && name != b".." {
                visit(OsStr::from_bytes(name))?;
            }
        }
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        File::from(
            openat(self.fd.as_ref(), ".", DIRECTORY_FLAGS, Mode::empty())
                .map_err(anyhow::Error::new)?,
        )
        .sync_all()
        .with_context(|| format!("sync anchored directory {}", self.display.display()))
    }

    pub(crate) fn available_space(&self) -> Result<u64> {
        let stats = rustix::fs::fstatvfs(self.fd.as_ref()).map_err(anyhow::Error::new)?;
        Ok(stats.f_bavail.saturating_mul(stats.f_frsize as u64))
    }

    pub(crate) fn display(&self) -> &Path {
        &self.display
    }

    fn validate_owner_mode(&self, expected_mode: u32) -> Result<()> {
        let metadata = File::from(
            openat(self.fd.as_ref(), ".", DIRECTORY_FLAGS, Mode::empty())
                .map_err(anyhow::Error::new)?,
        )
        .metadata()?;
        crate::writeback::validate_owner_only(
            &self.display,
            &metadata,
            expected_mode,
            "writeback directory",
        )
    }

    fn converge_owned_mode(&self, expected_mode: u32) -> Result<()> {
        let directory = File::from(
            openat(self.fd.as_ref(), ".", DIRECTORY_FLAGS, Mode::empty())
                .map_err(anyhow::Error::new)?,
        );
        let metadata = directory.metadata()?;
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!(
                "writeback directory {} is not owned by the service user",
                self.display.display()
            );
        }
        if metadata.permissions().mode() & 0o777 != expected_mode {
            directory
                .set_permissions(std::fs::Permissions::from_mode(expected_mode))
                .with_context(|| {
                    format!("set anchored directory mode for {}", self.display.display())
                })?;
        }
        Ok(())
    }

    fn set_mode(&self, mode: u32) -> Result<()> {
        File::from(
            openat(self.fd.as_ref(), ".", DIRECTORY_FLAGS, Mode::empty())
                .map_err(anyhow::Error::new)?,
        )
        .set_permissions(std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("set anchored directory mode for {}", self.display.display()))
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn descriptor_path(file: &File) -> PathBuf {
        use std::os::fd::AsRawFd;
        Path::new("/proc/self/fd").join(file.as_raw_fd().to_string())
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn descriptor_path(file: &File) -> PathBuf {
        use std::os::fd::AsRawFd;
        Path::new("/dev/fd").join(file.as_raw_fd().to_string())
    }
}

fn rustix_mode(mode: u32) -> Result<Mode> {
    match mode {
        0 => Ok(Mode::empty()),
        0o600 => Ok(Mode::RUSR.union(Mode::WUSR)),
        0o700 => Ok(Mode::RWXU),
        _ => bail!("unsupported private anchored file mode {mode:o}"),
    }
}

fn validate_component(name: &OsStr) -> Result<()> {
    let mut components = Path::new(name).components();
    let valid = matches!(components.next(), Some(Component::Normal(part)) if part == name)
        && components.next().is_none();
    if !valid {
        bail!("anchored path must be one safe component");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AnchoredDir, rustix_mode};
    use redb::{Database, DatabaseError, ReadOnlyDatabase, ReadableDatabase, TableDefinition};
    use rustix::fs::{Mode, OFlags};
    use std::os::unix::fs::PermissionsExt;

    fn owner_only(path: &std::path::Path) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn anchored_temp(root: &tempfile::TempDir) -> AnchoredDir {
        owner_only(root.path());
        AnchoredDir::open_or_create_absolute(&root.path().canonicalize().unwrap(), 0o700).unwrap()
    }

    #[test]
    fn rustix_mode_accepts_only_the_private_owner_modes() {
        assert_eq!(rustix_mode(0).unwrap(), Mode::empty());
        assert_eq!(rustix_mode(0o600).unwrap(), Mode::RUSR.union(Mode::WUSR));
        assert_eq!(rustix_mode(0o700).unwrap(), Mode::RWXU);
        assert!(rustix_mode(0o777).is_err());
    }

    #[test]
    fn parent_replacement_does_not_redirect_child_creation() {
        let root = tempfile::tempdir().unwrap();
        let physical_root = root.path().canonicalize().unwrap();
        let original = physical_root.join("original");
        let retained = physical_root.join("retained");
        let outside = physical_root.join("outside");
        std::fs::create_dir(&original).unwrap();
        std::fs::create_dir(&outside).unwrap();
        owner_only(&original);
        let anchored = AnchoredDir::open_or_create_absolute(&original, 0o700).unwrap();
        std::fs::rename(&original, &retained).unwrap();
        std::os::unix::fs::symlink(&outside, &original).unwrap();

        anchored
            .open_or_create_child("journal".as_ref(), 0o700)
            .unwrap();

        assert!(retained.join("journal").is_dir());
        assert!(!outside.join("journal").exists());
    }

    #[test]
    fn repeated_enumeration_starts_from_the_beginning() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a"), b"").unwrap();
        std::fs::write(root.path().join("b"), b"").unwrap();
        let anchored = anchored_temp(&root);

        let mut first = Vec::new();
        let mut second = Vec::new();
        anchored
            .for_each_entry(|name| {
                first.push(name.to_os_string());
                Ok(())
            })
            .unwrap();
        anchored
            .for_each_entry(|name| {
                second.push(name.to_os_string());
                Ok(())
            })
            .unwrap();
        first.sort();
        second.sort();
        assert_eq!(first, vec!["a", "b"]);
        assert_eq!(second, first);
    }

    #[test]
    fn create_child_converges_an_existing_owned_directory_to_owner_only() {
        let root = tempfile::tempdir().unwrap();
        let child = root.path().join("child");
        std::fs::create_dir(&child).unwrap();
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o775)).unwrap();
        let anchored = anchored_temp(&root);

        anchored
            .open_or_create_child("child".as_ref(), 0o700)
            .unwrap();

        assert_eq!(
            std::fs::symlink_metadata(child)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[test]
    fn absolute_parent_and_multi_component_names_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let anchored = anchored_temp(&root);
        for invalid in ["", ".", "/", "..", "a/", "a/b", "a//b"] {
            assert!(
                anchored
                    .open_or_create_child(invalid.as_ref(), 0o700)
                    .is_err()
            );
            assert!(
                anchored
                    .open_file(invalid.as_ref(), OFlags::RDONLY, 0)
                    .is_err()
            );
        }
    }

    #[test]
    fn descriptor_read_only_open_retains_identity_and_exact_writer_lock() {
        const IDENTITY: TableDefinition<u64, &str> = TableDefinition::new("identity");
        let root = tempfile::tempdir().unwrap();
        let anchored = anchored_temp(&root);
        let database_path = root.path().join("journal.redb");
        let retained_path = root.path().join("retained.redb");
        let original = Database::create(&database_path).unwrap();
        let write = original.begin_write().unwrap();
        write
            .open_table(IDENTITY)
            .unwrap()
            .insert(0, "original")
            .unwrap();
        write.commit().unwrap();
        drop(original);
        let pinned = anchored
            .open_file("journal.redb".as_ref(), OFlags::RDONLY, 0)
            .unwrap();
        std::fs::rename(&database_path, &retained_path).unwrap();
        let replacement = Database::create(&database_path).unwrap();
        let write = replacement.begin_write().unwrap();
        write
            .open_table(IDENTITY)
            .unwrap()
            .insert(0, "replacement")
            .unwrap();
        write.commit().unwrap();
        drop(replacement);

        let read_only = ReadOnlyDatabase::open(AnchoredDir::descriptor_path(&pinned)).unwrap();
        drop(pinned);
        let read = read_only.begin_read().unwrap();
        let table = read.open_table(IDENTITY).unwrap();
        assert_eq!(table.get(0).unwrap().unwrap().value(), "original");
        drop(table);
        drop(read);

        assert!(matches!(
            Database::create(&retained_path),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
        drop(read_only);
        Database::create(retained_path).unwrap();

        let replacement = ReadOnlyDatabase::open(database_path).unwrap();
        let read = replacement.begin_read().unwrap();
        let table = read.open_table(IDENTITY).unwrap();
        assert_eq!(table.get(0).unwrap().unwrap().value(), "replacement");
    }

    #[test]
    fn exclusive_owner_file_setup_failure_removes_the_anchored_file() {
        let root = tempfile::tempdir().unwrap();
        let anchored = anchored_temp(&root);

        let error = anchored
            .open_owner_file_with("container.blobs".as_ref(), false, |_| {
                Err(anyhow::anyhow!("injected owner-file setup failure"))
            })
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("injected owner-file setup failure"),
            "{error:#}"
        );
        assert!(!root.path().join("container.blobs").exists());
    }

    #[test]
    fn exclusive_owner_file_does_not_replace_an_existing_entry() {
        let root = tempfile::tempdir().unwrap();
        let existing = root.path().join("container.blobs");
        std::fs::write(&existing, b"outside").unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o600)).unwrap();
        let anchored = anchored_temp(&root);

        assert!(
            anchored
                .open_owner_file("container.blobs".as_ref(), false)
                .is_err()
        );
        assert_eq!(std::fs::read(existing).unwrap(), b"outside");
    }
}
