use super::plan::{PlannedFile, TransferPlan};
use crate::cli::attach_cleanup_errors;
use anyhow::{Context, Result, bail};
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, mkdirat, open, openat, renameat, statat, unlinkat,
};
use rustix::io::Errno;
use std::ffi::{OsStr, OsString};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const TEMP_FLAGS: OFlags = OFlags::WRONLY
    .union(OFlags::CREATE)
    .union(OFlags::EXCL)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const TEMP_NAME_ATTEMPTS: usize = 16;

#[derive(Clone)]
pub(super) struct LocalFileTarget {
    parent: Arc<OwnedFd>,
    name: OsString,
    path: PathBuf,
}

pub(super) struct LocalTemporaryFile {
    pub file: tokio::fs::File,
    pub name: OsString,
    pub path: PathBuf,
}

#[derive(Clone)]
pub(super) enum PreparedDownload {
    Directory { root: Arc<OwnedFd>, path: PathBuf },
    File(LocalFileTarget),
}

impl PreparedDownload {
    pub(super) fn prepare(
        plan: &TransferPlan,
        destination: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        preflight(plan, destination, cancellation)?;
        if cancellation.is_cancelled() {
            bail!("download cancelled");
        }
        if plan.source_is_dir {
            let root = walk_user_path(destination, true)?
                .context("local destination directory was not created")?;
            for relative in &plan.directories {
                if relative.as_os_str().is_empty() {
                    continue;
                }
                if cancellation.is_cancelled() {
                    bail!("download cancelled");
                }
                walk_relative(Arc::clone(&root), relative, true)?.with_context(|| {
                    format!(
                        "create local destination directory {}",
                        destination.join(relative).display()
                    )
                })?;
            }
            Ok(Self::Directory {
                root,
                path: destination.to_path_buf(),
            })
        } else {
            Ok(Self::File(prepare_file_target(destination)?))
        }
    }

    pub(super) fn target(&self, file: &PlannedFile) -> Result<LocalFileTarget> {
        match self {
            Self::File(target) => Ok(target.clone()),
            Self::Directory { root, path } => {
                let parent_path = file.relative.parent().unwrap_or_else(|| Path::new(""));
                let parent = walk_relative(Arc::clone(root), parent_path, false)?
                    .context("planned local destination parent is missing")?;
                let name = file
                    .relative
                    .file_name()
                    .context("planned local destination file has no name")?
                    .to_os_string();
                Ok(LocalFileTarget {
                    parent,
                    name,
                    path: path.join(&file.relative),
                })
            }
        }
    }
}

impl LocalFileTarget {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn create_temporary(&self) -> Result<LocalTemporaryFile> {
        for _ in 0..TEMP_NAME_ATTEMPTS {
            let name = OsString::from(format!(".zerofs-{}.tmp", Uuid::new_v4()));
            match openat(
                self.parent.as_ref(),
                &name,
                TEMP_FLAGS,
                Mode::from_raw_mode(0o644),
            ) {
                Ok(fd) => {
                    let path = self.path.with_file_name(&name);
                    return Ok(LocalTemporaryFile {
                        file: tokio::fs::File::from_std(std::fs::File::from(fd)),
                        name,
                        path,
                    });
                }
                Err(Errno::EXIST) => continue,
                Err(error) => {
                    return Err(anyhow::Error::new(error).context(format!(
                        "create local temporary file beside {}",
                        self.path.display()
                    )));
                }
            }
        }
        bail!(
            "could not allocate a unique local temporary file beside {}",
            self.path.display()
        )
    }

    pub(super) fn cleanup_temporary(
        &self,
        name: &OsStr,
        path: &Path,
        primary: anyhow::Error,
    ) -> anyhow::Error {
        let cleanup = match unlinkat(self.parent.as_ref(), name, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => Vec::new(),
            Err(error) => vec![
                anyhow::Error::new(error)
                    .context(format!("remove local temporary file {}", path.display())),
            ],
        };
        attach_cleanup_errors(primary, cleanup)
    }

    pub(super) fn publish_temporary(&self, name: &OsStr, path: &Path) -> Result<()> {
        renameat(self.parent.as_ref(), name, self.parent.as_ref(), &self.name)
            .map_err(anyhow::Error::new)
            .with_context(|| {
                format!(
                    "publish local file {} as {}",
                    path.display(),
                    self.path.display()
                )
            })
    }
}

fn preflight(
    plan: &TransferPlan,
    destination: &Path,
    cancellation: &CancellationToken,
) -> Result<()> {
    if !plan.source_is_dir {
        return preflight_file_target(destination);
    }
    let Some(root) = walk_user_path(destination, false)? else {
        return Ok(());
    };
    for relative in &plan.directories {
        if relative.as_os_str().is_empty() {
            continue;
        }
        if cancellation.is_cancelled() {
            bail!("download cancelled");
        }
        walk_relative(Arc::clone(&root), relative, false)?;
    }
    for file in &plan.files {
        if cancellation.is_cancelled() {
            bail!("download cancelled");
        }
        preflight_relative_file(&root, destination, file)?;
    }
    Ok(())
}

fn preflight_relative_file(
    root: &Arc<OwnedFd>,
    destination: &Path,
    file: &PlannedFile,
) -> Result<()> {
    let parent_path = file.relative.parent().unwrap_or_else(|| Path::new(""));
    let Some(parent) = walk_relative(Arc::clone(root), parent_path, false)? else {
        return Ok(());
    };
    let name = file
        .relative
        .file_name()
        .context("planned local destination file has no name")?;
    validate_file_entry(&parent, name, &destination.join(&file.relative))
}

fn preflight_file_target(destination: &Path) -> Result<()> {
    let (parent_path, name) = split_file_target(destination)?;
    let Some(parent) = walk_user_path(&parent_path, false)? else {
        return Ok(());
    };
    validate_file_entry(&parent, &name, destination)
}

fn prepare_file_target(destination: &Path) -> Result<LocalFileTarget> {
    let (parent_path, name) = split_file_target(destination)?;
    let parent =
        walk_user_path(&parent_path, true)?.context("local destination parent was not created")?;
    Ok(LocalFileTarget {
        parent,
        name,
        path: destination.to_path_buf(),
    })
}

fn split_file_target(path: &Path) -> Result<(PathBuf, OsString)> {
    let name = path
        .file_name()
        .with_context(|| format!("local destination has no file name: {}", path.display()))?
        .to_os_string();
    Ok((
        path.parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
        name,
    ))
}

fn validate_file_entry(parent: &OwnedFd, name: &OsStr, path: &Path) -> Result<()> {
    let metadata = match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(Errno::NOENT) => return Ok(()),
        Err(error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("inspect local destination {}", path.display())));
        }
    };
    let file_type = FileType::from_raw_mode(metadata.st_mode);
    if file_type.is_dir() {
        bail!("local destination is a directory: {}", path.display());
    }
    if file_type.is_symlink() {
        bail!(
            "local destination contains a symbolic link: {}",
            path.display()
        );
    }
    if !file_type.is_file() {
        bail!("unsupported local destination entry: {}", path.display());
    }
    Ok(())
}

fn walk_user_path(path: &Path, create: bool) -> Result<Option<Arc<OwnedFd>>> {
    let start = if path.is_absolute() { "/" } else { "." };
    let root = Arc::new(
        open(start, DIRECTORY_FLAGS, Mode::empty())
            .map_err(anyhow::Error::new)
            .with_context(|| format!("open local destination anchor {start}"))?,
    );
    let normalized = normalize_macos_root_alias(path);
    walk_components(root, normalized.components(), create, false, path)
}

#[cfg(target_os = "macos")]
fn normalize_macos_root_alias(path: &Path) -> PathBuf {
    for (alias, canonical) in [
        (Path::new("/var"), Path::new("/private/var")),
        (Path::new("/tmp"), Path::new("/private/tmp")),
        (Path::new("/etc"), Path::new("/private/etc")),
        (Path::new("/home"), Path::new("/System/Volumes/Data/home")),
    ] {
        if let Ok(remainder) = path.strip_prefix(alias) {
            return canonical.join(remainder);
        }
    }
    path.to_path_buf()
}

#[cfg(not(target_os = "macos"))]
fn normalize_macos_root_alias(path: &Path) -> PathBuf {
    path.to_path_buf()
}

fn walk_relative(root: Arc<OwnedFd>, path: &Path, create: bool) -> Result<Option<Arc<OwnedFd>>> {
    walk_components(root, path.components(), create, true, path)
}

fn walk_components<'a>(
    mut current: Arc<OwnedFd>,
    components: impl Iterator<Item = Component<'a>>,
    create: bool,
    relative_only: bool,
    display: &Path,
) -> Result<Option<Arc<OwnedFd>>> {
    let mut walked = PathBuf::new();
    for component in components {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::ParentDir if !relative_only => OsStr::new(".."),
            Component::Normal(name) => name,
            Component::ParentDir | Component::Prefix(_) => {
                bail!(
                    "local destination path escapes its transfer root: {}",
                    display.display()
                )
            }
        };
        walked.push(name);
        match open_child_directory(&current, name, create, &walked)? {
            Some(next) => current = Arc::new(next),
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

fn open_child_directory(
    parent: &OwnedFd,
    name: &OsStr,
    create: bool,
    display: &Path,
) -> Result<Option<OwnedFd>> {
    match openat(parent, name, DIRECTORY_FLAGS, Mode::empty()) {
        Ok(fd) => return Ok(Some(fd)),
        Err(Errno::NOENT) if !create => return Ok(None),
        Err(Errno::NOENT) => {}
        Err(Errno::LOOP | Errno::NOTDIR) => {
            bail!(
                "local destination contains a symbolic link or non-directory ancestor: {}",
                display.display()
            )
        }
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!(
                "open local destination directory {}",
                display.display()
            )));
        }
    }

    match mkdirat(parent, name, Mode::from_raw_mode(0o755)) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!(
                "create local destination directory {}",
                display.display()
            )));
        }
    }
    match openat(parent, name, DIRECTORY_FLAGS, Mode::empty()) {
        Ok(fd) => Ok(Some(fd)),
        Err(Errno::LOOP | Errno::NOTDIR) => bail!(
            "local destination contains a symbolic link or non-directory ancestor: {}",
            display.display()
        ),
        Err(error) => Err(anyhow::Error::new(error).context(format!(
            "open local destination directory {}",
            display.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::PreparedDownload;
    use crate::cli::transfer::plan::{PlannedFile, TransferPlan};
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;
    use tokio::io::AsyncWriteExt;
    use tokio_util::sync::CancellationToken;

    fn single_file_plan() -> TransferPlan {
        TransferPlan {
            source_is_dir: false,
            directories: Vec::new(),
            files: vec![PlannedFile {
                source: PathBuf::from("/remote/source.bin"),
                relative: PathBuf::new(),
                size: 7,
            }],
            total_bytes: 7,
        }
    }

    #[test]
    fn single_file_preflight_rejects_every_existing_symlinked_ancestor() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        let root = temp.path().join("root");
        fs::create_dir_all(&outside).unwrap();
        fs::create_dir_all(&root).unwrap();
        symlink(&outside, root.join("link")).unwrap();

        let error = PreparedDownload::prepare(
            &single_file_plan(),
            &root.join("link/nested/result.bin"),
            &CancellationToken::new(),
        )
        .err()
        .expect("symlinked ancestor must fail closed");

        assert!(error.to_string().contains("symbolic link"), "{error:#}");
        assert!(!outside.join("nested/result.bin").exists());
    }

    #[tokio::test]
    async fn held_parent_handle_prevents_ancestor_exchange_escape() {
        let temp = tempfile::tempdir().unwrap();
        let destination_parent = temp.path().join("destination");
        let renamed_parent = temp.path().join("held");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&destination_parent).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let prepared = PreparedDownload::prepare(
            &single_file_plan(),
            &destination_parent.join("result.bin"),
            &CancellationToken::new(),
        )
        .unwrap();
        let target = prepared.target(&single_file_plan().files[0]).unwrap();

        fs::rename(&destination_parent, &renamed_parent).unwrap();
        symlink(&outside, &destination_parent).unwrap();

        let mut temporary = target.create_temporary().unwrap();
        temporary.file.write_all(b"payload").await.unwrap();
        temporary.file.sync_all().await.unwrap();
        let name = temporary.name;
        let path = temporary.path;
        drop(temporary.file);
        target.publish_temporary(&name, &path).unwrap();

        assert_eq!(
            fs::read(renamed_parent.join("result.bin")).unwrap(),
            b"payload"
        );
        assert!(!outside.join("result.bin").exists());
    }

    #[test]
    fn local_cleanup_failure_is_attached_and_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("destination");
        fs::create_dir_all(&parent).unwrap();
        let prepared = PreparedDownload::prepare(
            &single_file_plan(),
            &parent.join("result.bin"),
            &CancellationToken::new(),
        )
        .unwrap();
        let target = prepared.target(&single_file_plan().files[0]).unwrap();
        let temporary = target.create_temporary().unwrap();
        let name = temporary.name;
        let path = temporary.path;
        drop(temporary.file);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        let error = target.cleanup_temporary(&name, &path, anyhow::anyhow!("primary failure"));

        let message = format!("{error:#}");
        assert!(message.contains("primary failure"), "{message}");
        assert!(message.contains("cleanup also failed"), "{message}");
        assert!(path.is_dir());
        fs::remove_dir(path).unwrap();
    }
}
