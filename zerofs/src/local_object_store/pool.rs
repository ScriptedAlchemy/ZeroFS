//! Native staging cleanup for catalog-owned, immutable pool generations.
//!
//! Ordinary objects and native multipart uploads do not use this path. The
//! pool persists each fresh UUID before starting its single native PUT and
//! excludes live upload owners before recovery. That durable record owns both
//! the final name and LocalFileSystem's exact `name#<positive decimal>` staging
//! siblings. Never infer ownership from a directory-wide staging pattern.

use super::{LOCK_FILE, generic, run_local};
use fs4::fs_std::FileExt;
use object_store::path::Path;
use object_store::{Error, Result};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, open, openat, statat, unlinkat};
use rustix::io::Errno;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path as FsPath};

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// Only pool-created physical UUID keys may opt into the checked-rename path.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PoolObjectCreate;

#[derive(Clone, Copy, Debug)]
pub(crate) enum GenerationState {
    /// A committed fresh UUID had one native PUT: only its final name and the
    /// original backend's possible `#1` hard-link remainder need removal.
    Published,
    /// Keep interrupted/failed uploads charged until every exact staging
    /// sibling has been reconciled, including gaps from a prior cleanup crash.
    Unpublished,
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

/// Called with the native directory mutation lock held. The pool never retries
/// a physical generation: a failed attempt is cleaned before its debt is freed.
/// Reject a preexisting first stage instead of allowing upstream to pick `#2`.
pub(super) fn ensure_fresh(path: &FsPath, location: &Path) -> Result<()> {
    let objects = path.parent();
    let pool = objects.and_then(FsPath::parent);
    let namespace = pool.and_then(FsPath::parent);
    if !path.is_absolute()
        || !path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(canonical_uuid)
        || objects.and_then(FsPath::file_name) != Some(OsStr::new("objects"))
        || !pool
            .and_then(FsPath::file_name)
            .and_then(OsStr::to_str)
            .is_some_and(canonical_uuid)
        || namespace.and_then(FsPath::file_name) != Some(OsStr::new(".zerofs-pool"))
    {
        return Err(generic(
            "pool native create requires a canonical physical UUID key",
        ));
    }
    let mut staging = path.as_os_str().to_owned();
    staging.push("#1");
    for candidate in [path, FsPath::new(&staging)] {
        match std::fs::symlink_metadata(candidate) {
            Ok(_) => {
                return Err(Error::AlreadyExists {
                    path: location.to_string(),
                    source: "pool physical generation or its staging file already exists".into(),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(generic(error)),
        }
    }
    Ok(())
}

fn io_error(error: Errno) -> Error {
    generic(std::io::Error::from(error))
}

fn staging_name(name: &[u8], object: &[u8]) -> bool {
    name.strip_prefix(object)
        .and_then(|suffix| suffix.strip_prefix(b"#"))
        .is_some_and(|suffix| {
            matches!(suffix.first(), Some(b'1'..=b'9')) && suffix.iter().all(u8::is_ascii_digit)
        })
}

/// Reclaim only a quiescent generation already owned by a durable upload or
/// garbage record. The caller must retain that record until this returns Ok.
/// The detached native worker retains the OS lock through directory fsync even
/// if recovery's bounded async wait is cancelled. Local pool roots are canonical
/// absolute paths; walking every component with NOFOLLOW prevents cleanup from
/// escaping through a replaced pool subdirectory.
pub(crate) async fn remove<Guard: Send + 'static>(
    root: &FsPath,
    physical_key: &str,
    state: GenerationState,
    ownership: Guard,
) -> Result<()> {
    let components: Vec<_> = physical_key.split('/').collect();
    if components.len() != 4
        || components[0] != ".zerofs-pool"
        || !canonical_uuid(components[1])
        || components[2] != "objects"
        || !canonical_uuid(components[3])
    {
        return Err(generic(
            "native pool cleanup requires a canonical physical UUID key",
        ));
    }
    if !root.is_absolute() {
        return Err(generic(
            "native pool cleanup requires an absolute member root",
        ));
    }
    let root = root.to_owned();
    let physical_key = physical_key.to_owned();
    run_local(move || {
        // Status and shutdown ownership follow the actual native operation,
        // including cancellation of the async receiver while this worker waits.
        let _ownership = ownership;
        let mut directory =
            File::from(open("/", DIRECTORY_FLAGS, Mode::empty()).map_err(io_error)?);
        for component in root.components() {
            match component {
                Component::RootDir => {}
                Component::Normal(name) => {
                    directory = File::from(
                        openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())
                            .map_err(io_error)?,
                    );
                }
                _ => return Err(generic("native pool member root must be canonical")),
            }
        }
        let components: Vec<_> = physical_key.split('/').collect();
        for name in &components[..3] {
            match openat(&directory, *name, DIRECTORY_FLAGS, Mode::empty()) {
                Ok(next) => directory = File::from(next),
                Err(Errno::NOENT) => {
                    // A started upload can crash before it creates objects/.
                    // Make the observed absence durable before retiring debt.
                    return directory.sync_all().map_err(generic);
                }
                Err(error) => return Err(io_error(error)),
            }
        }
        let lock = File::from(
            openat(
                &directory,
                LOCK_FILE,
                OFlags::CREATE
                    .union(OFlags::RDWR)
                    .union(OFlags::NOFOLLOW)
                    .union(OFlags::NONBLOCK)
                    .union(OFlags::CLOEXEC),
                Mode::RUSR.union(Mode::WUSR),
            )
            .map_err(io_error)?,
        );
        if !lock.metadata().map_err(generic)?.is_file() {
            return Err(generic("native pool mutation lock is not a regular file"));
        }
        FileExt::lock_exclusive(&lock).map_err(generic)?;
        lock.sync_all().map_err(generic)?;
        directory.sync_all().map_err(generic)?;

        let object = components[3].as_bytes();
        let names: Vec<OsString> = match state {
            // Every successful foreground deletion/evacuation retires a
            // published generation. Avoid a full directory scan per object.
            GenerationState::Published => {
                vec![components[3].into(), format!("{}#1", components[3]).into()]
            }
            GenerationState::Unpublished => {
                let scan =
                    openat(&directory, ".", DIRECTORY_FLAGS, Mode::empty()).map_err(io_error)?;
                let mut names = Vec::new();
                for entry in Dir::new(scan).map_err(io_error)? {
                    let entry = entry.map_err(io_error)?;
                    let name = entry.file_name().to_bytes();
                    if name == object || staging_name(name, object) {
                        names.push(OsStr::from_bytes(name).to_owned());
                    }
                }
                names
            }
        };
        // Finish enumeration before mutating the directory, so removing one
        // entry cannot cause a filesystem's iterator to skip another stage.
        for name in names {
            let metadata = match statat(&directory, &name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(metadata) => metadata,
                Err(Errno::NOENT) => continue,
                Err(error) => return Err(io_error(error)),
            };
            if !FileType::from_raw_mode(metadata.st_mode).is_file() {
                return Err(generic(
                    "native pool cleanup found a non-regular owned object",
                ));
            }
            match unlinkat(&directory, &name, AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        directory.sync_all().map_err(generic)
    })
    .await
}

#[cfg(test)]
mod tests;
