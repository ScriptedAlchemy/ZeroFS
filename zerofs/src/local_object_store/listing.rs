//! List only the requested subtree, rejecting links during the same walk.

use super::{generic, run_local};
use crate::storage_readiness::paths::{ScanProgress, existing_ancestor};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::path::Path;
use object_store::{ListResult, ObjectMeta, Result};
use std::collections::VecDeque;
use std::os::unix::fs::MetadataExt;
use std::path::{Path as FsPath, PathBuf};
use std::time::SystemTime;
use walkdir::WalkDir;

enum Entry {
    Directory(Path),
    Object(ObjectMeta),
}

struct Listing {
    entries: walkdir::IntoIter,
    check_prefix: Option<(PathBuf, PathBuf)>,
    offset: Option<Path>,
    progress: ScanProgress,
    finished: bool,
}

impl Listing {
    fn batch(&mut self) -> Result<VecDeque<Entry>> {
        if let Some((root, base)) = self.check_prefix.take() {
            existing_ancestor(&root, &base).map_err(generic)?;
        }
        let mut batch = VecDeque::new();
        for _ in 0..1024 {
            let Some(entry) = self.entries.next() else {
                self.finished = true;
                break;
            };
            self.progress.entry();
            let entry = match entry {
                Ok(entry) => entry,
                Err(error)
                    if error
                        .io_error()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(error) => return Err(generic(error)),
            };
            // Check directories before applying the offset or hidden-file
            // filters. A skipped name must not hide a symlinked subtree.
            if entry.file_type().is_symlink() {
                return Err(generic(format!(
                    "readiness path contains a symlink: {}",
                    entry.path().display()
                )));
            }
            let location = Path::from_absolute_path(entry.path()).map_err(generic)?;
            if entry.file_type().is_dir() {
                batch.push_back(Entry::Directory(location));
                continue;
            }
            if !entry.file_type().is_file()
                || self
                    .offset
                    .as_ref()
                    .is_some_and(|offset| location <= *offset)
                || hidden(&location)
            {
                continue;
            }
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error)
                    if error
                        .io_error()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(error) => return Err(generic(error)),
            };
            // Match LocalFileSystem's metadata and ETag without reopening each
            // object or performing a second directory walk.
            let modified = metadata.modified().map_err(generic)?;
            let mtime = modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros();
            batch.push_back(Entry::Object(ObjectMeta {
                location,
                last_modified: modified.into(),
                size: metadata.len(),
                e_tag: Some(format!(
                    "\"{:x}-{mtime:x}-{:x}\"",
                    metadata.ino(),
                    metadata.len()
                )),
                version: None,
            }));
        }
        Ok(batch)
    }
}

fn hidden(location: &Path) -> bool {
    location
        .filename()
        .and_then(|name| name.split_once('#'))
        .is_some_and(|(_, suffix)| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn walk(
    root: &FsPath,
    prefix: Option<&Path>,
    offset: Option<&Path>,
    recursive: bool,
) -> BoxStream<'static, Result<Entry>> {
    // LocalConditionalStore uses absolute paths under LocalFileSystem::new().
    // A directory prefix may end in #<digits>, unlike a native object path.
    let base = match prefix.filter(|prefix| !prefix.as_ref().is_empty()) {
        Some(prefix) => FsPath::new("/").join(prefix.as_ref()),
        None => root.to_owned(),
    };
    let mut entries = WalkDir::new(&base).min_depth(1).follow_links(false);
    if !recursive {
        entries = entries.max_depth(1);
    }
    let listing = Listing {
        entries: entries.into_iter(),
        check_prefix: Some((root.to_owned(), base.clone())),
        offset: offset.cloned(),
        progress: ScanProgress::new("listing local objects", &base),
        finished: false,
    };
    futures::stream::try_unfold(
        (listing, VecDeque::new()),
        |(mut listing, mut buffer)| async move {
            loop {
                if let Some(entry) = buffer.pop_front() {
                    return Ok(Some((entry, (listing, buffer))));
                }
                if listing.finished {
                    return Ok(None);
                }
                (listing, buffer) = run_local(move || {
                    let batch = listing.batch()?;
                    Ok((listing, batch))
                })
                .await?;
            }
        },
    )
    .boxed()
}

pub(super) fn list(
    root: &FsPath,
    prefix: Option<&Path>,
    offset: Option<&Path>,
) -> BoxStream<'static, Result<ObjectMeta>> {
    walk(root, prefix, offset, true)
        .try_filter_map(|entry| async {
            Ok(match entry {
                Entry::Object(meta) => Some(meta),
                Entry::Directory(_) => None,
            })
        })
        .boxed()
}

pub(super) async fn list_with_delimiter(
    root: &FsPath,
    prefix: Option<&Path>,
) -> Result<ListResult> {
    let mut entries = walk(root, prefix, None, false);
    let mut result = ListResult {
        common_prefixes: Vec::new(),
        objects: Vec::new(),
        extensions: Default::default(),
    };
    while let Some(entry) = entries.try_next().await? {
        match entry {
            Entry::Directory(path) => result.common_prefixes.push(path),
            Entry::Object(meta) => result.objects.push(meta),
        }
    }
    result.common_prefixes.sort();
    Ok(result)
}
