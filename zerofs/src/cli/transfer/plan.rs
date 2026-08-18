use anyhow::{Context, Result, bail};
use std::collections::VecDeque;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use zerofs_client::{Client, FileType};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PlannedFile {
    pub source: PathBuf,
    pub relative: PathBuf,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TransferPlan {
    pub source_is_dir: bool,
    pub directories: Vec<PathBuf>,
    pub files: Vec<PlannedFile>,
    pub total_bytes: u64,
}

pub(super) fn scan_local(source: &Path) -> Result<TransferPlan> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("inspect local source {}", source.display()))?;
    if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
        bail!("unsupported source entry: {}", source.display());
    }
    if metadata.is_file() {
        return Ok(TransferPlan {
            source_is_dir: false,
            directories: Vec::new(),
            files: vec![PlannedFile {
                source: source.to_path_buf(),
                relative: PathBuf::new(),
                size: metadata.len(),
            }],
            total_bytes: metadata.len(),
        });
    }

    let mut plan = TransferPlan {
        source_is_dir: true,
        directories: vec![PathBuf::new()],
        files: Vec::new(),
        total_bytes: 0,
    };
    scan_local_directory(source, source, &mut plan)?;
    Ok(sort_plan(plan))
}

fn scan_local_directory(root: &Path, directory: &Path, plan: &mut TransferPlan) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .with_context(|| format!("read local directory {}", directory.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .context("local source escaped its transfer root")?
            .to_path_buf();
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect local source entry {}", path.display()))?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            bail!("unsupported source entry: {}", path.display());
        }
        if metadata.is_dir() {
            plan.directories.push(relative);
            scan_local_directory(root, &path, plan)?;
        } else {
            plan.total_bytes = plan
                .total_bytes
                .checked_add(metadata.len())
                .context("local transfer size exceeds u64")?;
            plan.files.push(PlannedFile {
                source: path,
                relative,
                size: metadata.len(),
            });
        }
    }
    Ok(())
}

pub(super) async fn scan_remote(client: &Client, source: &Path) -> Result<TransferPlan> {
    let metadata = client
        .stat(source)
        .await
        .with_context(|| format!("inspect remote source {}", source.display()))?;
    match metadata.file_type {
        FileType::File => Ok(TransferPlan {
            source_is_dir: false,
            directories: Vec::new(),
            files: vec![PlannedFile {
                source: source.to_path_buf(),
                relative: PathBuf::new(),
                size: metadata.size,
            }],
            total_bytes: metadata.size,
        }),
        FileType::Dir => scan_remote_directory(client, source).await,
        _ => bail!("unsupported source entry: {}", source.display()),
    }
}

async fn scan_remote_directory(client: &Client, root: &Path) -> Result<TransferPlan> {
    let mut plan = TransferPlan {
        source_is_dir: true,
        directories: vec![PathBuf::new()],
        files: Vec::new(),
        total_bytes: 0,
    };
    let mut pending = VecDeque::from([(root.to_path_buf(), PathBuf::new())]);

    while let Some((directory, relative_directory)) = pending.pop_front() {
        let mut entries = client
            .read_dir(&directory)
            .await
            .with_context(|| format!("read remote directory {}", directory.display()))?;
        entries.sort_by(|left, right| left.name_bytes.cmp(&right.name_bytes));
        for entry in entries {
            validate_remote_child_name(&entry.name_bytes, &directory)?;
            let name = std::ffi::OsString::from_vec(entry.name_bytes);
            let remote_path = directory.join(&name);
            let relative = relative_directory.join(name);
            match entry.file_type {
                FileType::Dir => {
                    plan.directories.push(relative.clone());
                    pending.push_back((remote_path, relative));
                }
                FileType::File => {
                    plan.total_bytes = plan
                        .total_bytes
                        .checked_add(entry.metadata.size)
                        .context("remote transfer size exceeds u64")?;
                    plan.files.push(PlannedFile {
                        source: remote_path,
                        relative,
                        size: entry.metadata.size,
                    });
                }
                _ => bail!("unsupported source entry: {}", remote_path.display()),
            }
        }
    }

    Ok(sort_plan(plan))
}

pub(super) fn validate_remote_child_name(name: &[u8], directory: &Path) -> Result<()> {
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') || name.contains(&0)
    {
        bail!("invalid remote directory entry in {}", directory.display());
    }
    Ok(())
}

fn sort_plan(mut plan: TransferPlan) -> TransferPlan {
    plan.directories.sort_by(|left, right| {
        left.components()
            .count()
            .cmp(&right.components().count())
            .then_with(|| left.cmp(right))
    });
    plan.files
        .sort_by(|left, right| left.relative.cmp(&right.relative));
    plan
}

#[cfg(test)]
mod tests {
    use super::{scan_local, scan_remote};
    use crate::fs::ZeroFS;
    use crate::ninep::NinePServer;
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use zerofs_client::Client;

    async fn remote_client() -> (Arc<Client>, CancellationToken, tempfile::TempDir) {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("transfer-plan.9p.sock");
        let server = NinePServer::new_unix(filesystem, socket.clone());
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move { server.start(server_shutdown).await.unwrap() });
        let target = format!("unix:{}", socket.display());
        for _ in 0..100 {
            if let Ok(client) = Client::connect(&target).await {
                return (client, shutdown, temp);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("test 9P client did not connect");
    }

    #[test]
    fn local_plan_counts_bytes_and_keeps_empty_directories() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        fs::create_dir_all(source.join("nested/empty")).unwrap();
        fs::write(source.join("root.bin"), b"root").unwrap();
        fs::write(source.join("nested/child.bin"), b"child").unwrap();

        let plan = scan_local(&source).unwrap();

        assert!(plan.source_is_dir);
        assert_eq!(plan.total_bytes, 9);
        assert_eq!(
            plan.directories,
            ["", "nested", "nested/empty"]
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            plan.files
                .iter()
                .map(|file| (file.relative.to_string_lossy().into_owned(), file.size))
                .collect::<Vec<_>>(),
            [("nested/child.bin".into(), 5), ("root.bin".into(), 4)]
        );
    }

    #[test]
    fn local_plan_rejects_symlinks_instead_of_following_them() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("real.bin"), b"payload").unwrap();
        symlink("real.bin", source.join("link.bin")).unwrap();

        let error = scan_local(&source).unwrap_err();

        assert!(
            error.to_string().contains("unsupported source entry"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn remote_plan_counts_bytes_and_keeps_empty_directories() {
        let (client, _shutdown, _temp) = remote_client().await;
        client
            .create_dir_all("/source/nested/empty", 0o755)
            .await
            .unwrap();
        client.write("/source/root.bin", b"root").await.unwrap();
        client
            .write("/source/nested/child.bin", b"child")
            .await
            .unwrap();

        let plan = scan_remote(&client, std::path::Path::new("/source"))
            .await
            .unwrap();

        assert!(plan.source_is_dir);
        assert_eq!(plan.total_bytes, 9);
        assert_eq!(
            plan.directories,
            ["", "nested", "nested/empty"]
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            plan.files
                .iter()
                .map(|file| (file.relative.to_string_lossy().into_owned(), file.size))
                .collect::<Vec<_>>(),
            [("nested/child.bin".into(), 5), ("root.bin".into(), 4)]
        );
    }
}
