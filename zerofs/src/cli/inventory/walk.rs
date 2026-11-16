use super::Manifest;
use super::collect;
use super::platform;
use super::record::*;
use anyhow::{Context, Result, bail};
use rustix::fd::{AsFd, BorrowedFd, OwnedFd};
use rustix::fs::{self, AtFlags, Dir, Mode, OFlags, SeekFrom, Stat};
use rustix::io::Errno;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

struct Links {
    nlink: u64,
    consistent: bool,
    paths: Vec<Vec<u8>>,
}

struct Walker<'a, W> {
    output: &'a mut Manifest<W>,
    root_dev: u64,
    hash: bool,
    tree_complete: bool,
    links: BTreeMap<(u64, u64), Links>,
    derived: Vec<Record>,
}

pub(super) fn inventory(root: &Path, hash: bool, writer: impl Write) -> Result<bool> {
    // A trailing slash must not bypass O_NOFOLLOW on the final component.
    let raw = root.as_os_str().as_bytes();
    let end = raw
        .iter()
        .rposition(|b| *b != b'/')
        .map_or(raw.len().min(1), |i| i + 1);
    let path = OsStr::from_bytes(&raw[..end]);
    let before =
        fs::statat(fs::CWD, path, AtFlags::SYMLINK_NOFOLLOW).context("stat inventory root")?;
    if kind(&before) != EntryKind::Directory {
        bail!("inventory root must be a directory, not a symlink or file");
    }
    let root_fd = fs::open(path, DIRECTORY_FLAGS, Mode::empty())
        .context("open inventory root without following symlinks")?;
    let opened = fs::fstat(&root_fd).context("stat opened inventory root")?;
    if !same_version(&before, &opened) {
        bail!("inventory root changed while opening");
    }
    let mut output = Manifest::new(writer);
    let filesystem = match fs::fstatfs(&root_fd) {
        Ok(stats) => Field::Observed(platform::filesystem(&stats)),
        Err(error) => Field::error(error),
    };
    output.emit(&Record::Header(Header {
        schema: 1,
        tool_version: env!("CARGO_PKG_VERSION"),
        collector: std::env::consts::OS,
        root: RawBytes::new(raw),
        root_identity: Field::Observed(identity(&before)),
        filesystem,
        hashing: if hash { "sha256" } else { "not_requested" },
        collision_rules: super::collision::RULES,
        atime_notice: "Times are sampled before opening entries. Reads may update atime; only a read-only mount prevents this. This is not a filesystem snapshot.",
    }))?;
    let mut walker = Walker {
        output: &mut output,
        root_dev: identity(&before).dev,
        hash,
        tree_complete: true,
        links: BTreeMap::new(),
        derived: Vec::new(),
    };
    let mut entry = stat_entry(Vec::new(), &before, root_fd.as_fd(), OsStr::new("."));
    walker.collect_fd(&mut entry, root_fd.as_fd(), EntryKind::Directory);
    walker.output.emit(&Record::Entry(Box::new(entry)))?;
    walker.directory(root_fd.as_fd(), &[])?;
    walker.check_directory(root_fd.as_fd(), &[], &before);
    walker.finish()?;
    output.finish()
}

impl<W: Write> Walker<'_, W> {
    fn directory(&mut self, fd: BorrowedFd<'_>, path: &[u8]) -> Result<()> {
        let names = match names(fd) {
            Ok(names) => names,
            Err(error) => {
                self.unreadable(path, error);
                return Ok(());
            }
        };
        self.output.non_utf8_names += names
            .iter()
            .filter(|n| std::str::from_utf8(n).is_err())
            .count() as u64;
        for (rule, group) in super::collision::collisions(&names) {
            self.derived.push(Record::NameCollision(NameCollision {
                dir: RawBytes::new(path),
                rule,
                names: group.iter().map(|n| RawBytes::new(n)).collect(),
            }));
        }
        for name in names {
            let child = join(path, &name);
            self.entry(fd, OsStr::from_bytes(&name), child)?;
        }
        Ok(())
    }

    fn entry(&mut self, parent: BorrowedFd<'_>, name: &OsStr, path: Vec<u8>) -> Result<()> {
        let before = match fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(error) => {
                let entry = failed_entry(&path, error);
                self.tree_complete &= entry.kind.is_complete();
                self.output.emit(&Record::Entry(Box::new(entry)))?;
                return Ok(());
            }
        };
        let entry_kind = kind(&before);
        let mut entry = stat_entry(path.clone(), &before, parent, name);
        if identity(&before).dev != self.root_dev {
            self.tree_complete &= Reason::MountBoundary.counts_as_complete();
            entry.xattrs = Field::unavailable(Reason::MountBoundary);
            entry.acl = Field::unavailable(Reason::MountBoundary);
            entry.data_extents = Field::unavailable(Reason::MountBoundary);
            self.output.emit(&Record::Entry(Box::new(entry)))?;
            self.derived.push(Record::MountBoundary {
                path: RawBytes::new(&path),
                identity: Field::Observed(identity(&before)),
                error: Field::unavailable(Reason::MountBoundary),
            });
            return Ok(());
        }
        let fd = match entry_kind {
            EntryKind::Directory => Some(fs::openat(parent, name, DIRECTORY_FLAGS, Mode::empty())),
            EntryKind::File => Some(fs::openat(
                parent,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )),
            #[cfg(target_os = "macos")]
            EntryKind::Symlink => Some(fs::openat(
                parent,
                name,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::from_bits_retain(libc::O_SYMLINK as u32),
                Mode::empty(),
            )),
            _ => None,
        };
        let fd = match fd {
            Some(Ok(fd)) => match fs::fstat(&fd) {
                Ok(opened) if same_version(&before, &opened) => Some(fd),
                Ok(_) => {
                    self.changed(&path);
                    invalidate_reads(&mut entry, Reason::EntryChanged);
                    self.output.emit(&Record::Entry(Box::new(entry)))?;
                    return Ok(());
                }
                Err(error) => {
                    self.open_failed(&mut entry, entry_kind, &path, error);
                    None
                }
            },
            Some(Err(error)) => {
                self.open_failed(&mut entry, entry_kind, &path, error);
                None
            }
            None => None,
        };
        if let Some(fd) = &fd {
            self.collect_fd(&mut entry, fd.as_fd(), entry_kind);
        }
        if entry_kind == EntryKind::Symlink {
            #[cfg(target_os = "linux")]
            {
                (entry.xattrs, entry.acl) = platform::symlink_metadata(parent, name);
            }
            entry.symlink_target = match fs::readlinkat(parent, name, Vec::new()) {
                Ok(target) => Field::Observed(RawBytes::new(target.to_bytes())),
                Err(error) => Field::error(error),
            };
        }
        let after = if let Some(fd) = &fd {
            fs::fstat(fd)
        } else {
            fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
        };
        let stable = matches!(&after, Ok(after) if same_version(&before, after))
            && (entry_kind != EntryKind::Symlink
                || matches!(fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW), Ok(after) if same_version(&before, &after)));
        if !stable {
            self.changed(&path);
            invalidate_reads(&mut entry, Reason::EntryChanged);
        }
        if entry_kind != EntryKind::Directory && before.st_nlink > 1 {
            let id = identity(&before);
            let group = self.links.entry((id.dev, id.ino)).or_insert_with(|| Links {
                nlink: before.st_nlink as u64,
                consistent: true,
                paths: Vec::new(),
            });
            group.consistent &= stable && group.nlink == before.st_nlink as u64;
            group.paths.push(path.clone());
        }
        self.output.emit(&Record::Entry(Box::new(entry)))?;
        if stable
            && entry_kind == EntryKind::Directory
            && let Some(fd) = fd
        {
            self.directory(fd.as_fd(), &path)?;
            self.check_directory(fd.as_fd(), &path, &before);
        }
        Ok(())
    }

    fn collect_fd(&self, entry: &mut Entry, fd: BorrowedFd<'_>, kind: EntryKind) {
        entry.xattrs = collect::xattrs(fd);
        entry.acl = platform::acl(fd);
        if kind == EntryKind::File {
            if let Field::Observed(size) = entry.size {
                entry.data_extents = extents(fd, size);
            }
            if self.hash {
                entry.content_sha256 = content_hash(fd);
            }
        }
    }

    fn open_failed(&mut self, entry: &mut Entry, kind: EntryKind, path: &[u8], error: Errno) {
        entry.xattrs = Field::error(error);
        entry.acl = Field::error(error);
        if kind == EntryKind::File {
            entry.data_extents = Field::error(error);
            if self.hash {
                entry.content_sha256 = Field::error(error);
            }
        }
        if kind == EntryKind::Directory {
            self.unreadable(path, error);
        }
    }

    fn unreadable(&mut self, path: &[u8], error: Errno) {
        let error = Field::error(error);
        self.tree_complete &= error.is_complete();
        self.derived.push(Record::DirectoryUnreadable {
            path: RawBytes::new(path),
            error,
        });
    }

    fn changed(&mut self, path: &[u8]) {
        self.tree_complete &= Reason::EntryChanged.counts_as_complete();
        self.derived.push(Record::EntryChanged {
            path: RawBytes::new(path),
            error: Field::unavailable(Reason::EntryChanged),
        });
    }

    fn check_directory(&mut self, fd: BorrowedFd<'_>, path: &[u8], before: &Stat) {
        if !matches!(fs::fstat(fd), Ok(after) if same_version(before, &after)) {
            self.changed(path);
        }
    }

    fn finish(self) -> Result<()> {
        for ((dev, ino), mut links) in self.links {
            links.paths.sort_unstable();
            let count = links.paths.len() as u64;
            let nlink = if links.consistent && links.nlink >= count {
                Field::Observed(links.nlink)
            } else {
                Field::unavailable(Reason::EntryChanged)
            };
            let outside = if self.tree_complete && links.consistent && links.nlink >= count {
                Field::Observed(links.nlink - count)
            } else {
                Field::unavailable(Reason::IncompleteTraversal)
            };
            self.output.emit(&Record::HardlinkGroup(HardlinkGroup {
                dev,
                ino,
                nlink,
                paths_in_tree: links.paths.iter().map(|p| RawBytes::new(p)).collect(),
                links_outside_tree: outside,
            }))?;
        }
        for record in self.derived {
            self.output.emit(&record)?;
        }
        Ok(())
    }
}

fn names(fd: BorrowedFd<'_>) -> Result<Vec<Vec<u8>>, Errno> {
    let scan = fs::openat(fd, ".", DIRECTORY_FLAGS, Mode::empty())?;
    let directory = Dir::new(scan)?;
    let mut names = Vec::new();
    for entry in directory {
        let entry = entry?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec());
        }
    }
    names.sort_unstable();
    Ok(names)
}

fn join(parent: &[u8], name: &[u8]) -> Vec<u8> {
    let mut path = parent.to_vec();
    if !path.is_empty() {
        path.push(b'/');
    }
    path.extend_from_slice(name);
    path
}

fn kind(stat: &Stat) -> EntryKind {
    match fs::FileType::from_raw_mode(stat.st_mode) {
        fs::FileType::RegularFile => EntryKind::File,
        fs::FileType::Directory => EntryKind::Directory,
        fs::FileType::Symlink => EntryKind::Symlink,
        fs::FileType::Fifo => EntryKind::Fifo,
        fs::FileType::Socket => EntryKind::Socket,
        fs::FileType::CharacterDevice => EntryKind::CharacterDevice,
        fs::FileType::BlockDevice => EntryKind::BlockDevice,
        _ => EntryKind::Unknown,
    }
}

#[allow(clippy::unnecessary_cast)] // Darwin and Linux stat integer widths differ.
fn identity(stat: &Stat) -> Identity {
    Identity {
        dev: stat.st_dev as u64,
        ino: stat.st_ino as u64,
    }
}

#[allow(clippy::unnecessary_cast)]
fn stat_entry(path: Vec<u8>, stat: &Stat, parent: BorrowedFd<'_>, name: &OsStr) -> Entry {
    Entry {
        path: RawBytes::new(&path),
        kind: Field::Observed(kind(stat)),
        identity: Field::Observed(identity(stat)),
        nlink: Field::Observed(stat.st_nlink as u64),
        owner: Field::Observed(Owner {
            uid: stat.st_uid,
            gid: stat.st_gid,
        }),
        mode: Field::Observed(stat.st_mode as u32),
        times: Field::Observed(Times {
            access: Timestamp {
                seconds: stat.st_atime as i64,
                nanoseconds: stat.st_atime_nsec as u32,
            },
            modification: Timestamp {
                seconds: stat.st_mtime as i64,
                nanoseconds: stat.st_mtime_nsec as u32,
            },
            change: Timestamp {
                seconds: stat.st_ctime as i64,
                nanoseconds: stat.st_ctime_nsec as u32,
            },
        }),
        birth_time: platform::birth(parent, name, stat),
        size: Field::Observed(stat.st_size as u64),
        allocated_bytes: Field::Observed(stat.st_blocks as u64 * 512),
        rdev: Field::Observed(stat.st_rdev as u64),
        platform_flags: platform::flags(stat),
        data_extents: Field::unavailable(Reason::NotCollectedForKind),
        symlink_target: Field::unavailable(Reason::NotCollectedForKind),
        xattrs: Field::unavailable(Reason::NotCollectedForKind),
        acl: Field::unavailable(Reason::NotCollectedForKind),
        content_sha256: Field::unavailable(if kind(stat) == EntryKind::File {
            Reason::NotRequested
        } else {
            Reason::NotCollectedForKind
        }),
    }
}

fn failed_entry(path: &[u8], error: Errno) -> Entry {
    Entry {
        path: RawBytes::new(path),
        kind: Field::error(error),
        identity: Field::error(error),
        nlink: Field::error(error),
        owner: Field::error(error),
        mode: Field::error(error),
        times: Field::error(error),
        birth_time: Field::error(error),
        size: Field::error(error),
        allocated_bytes: Field::error(error),
        data_extents: Field::error(error),
        symlink_target: Field::error(error),
        rdev: Field::error(error),
        platform_flags: Field::error(error),
        xattrs: Field::error(error),
        acl: Field::error(error),
        content_sha256: Field::error(error),
    }
}

fn invalidate_reads(entry: &mut Entry, reason: Reason) {
    entry.xattrs = Field::unavailable(reason);
    entry.acl = Field::unavailable(reason);
    entry.data_extents = Field::unavailable(reason);
    entry.symlink_target = Field::unavailable(reason);
    entry.content_sha256 = Field::unavailable(reason);
}

// Reading the source may update its access time, so the comparison excludes it.
fn same_version(a: &Stat, b: &Stat) -> bool {
    identity(a) == identity(b)
        && a.st_mode == b.st_mode
        && a.st_nlink == b.st_nlink
        && a.st_uid == b.st_uid
        && a.st_gid == b.st_gid
        && a.st_size == b.st_size
        && a.st_blocks == b.st_blocks
        && a.st_rdev == b.st_rdev
        && a.st_mtime == b.st_mtime
        && a.st_mtime_nsec == b.st_mtime_nsec
        && a.st_ctime == b.st_ctime
        && a.st_ctime_nsec == b.st_ctime_nsec
}

fn extents(fd: BorrowedFd<'_>, size: u64) -> Field<Vec<Extent>> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    while offset < size {
        let data = match fs::seek(fd, SeekFrom::Data(offset)) {
            Ok(data) if data < offset => return Field::unavailable(Reason::EntryChanged),
            Ok(data) if data < size => data,
            Ok(_) | Err(Errno::NXIO) => break,
            Err(Errno::INVAL) => return Field::unavailable(Reason::NotSupportedByFilesystem),
            Err(error) => return Field::error(error),
        };
        let hole = match fs::seek(fd, SeekFrom::Hole(data)) {
            Ok(hole) if hole > data => hole.min(size),
            Ok(_) => return Field::unavailable(Reason::EntryChanged),
            Err(Errno::INVAL) => return Field::unavailable(Reason::NotSupportedByFilesystem),
            Err(error) => return Field::error(error),
        };
        ranges.push(Extent {
            offset: data,
            length: hole - data,
        });
        offset = hole;
    }
    Field::Observed(ranges)
}

fn content_hash(fd: BorrowedFd<'_>) -> Field<String> {
    let duplicate: OwnedFd = match rustix::io::dup(fd) {
        Ok(fd) => fd,
        Err(error) => return Field::error(error),
    };
    let mut file = std::fs::File::from(duplicate);
    use std::io::{Seek, SeekFrom};
    let result = (|| -> std::io::Result<String> {
        file.seek(SeekFrom::Start(0))?;
        let mut hasher = Sha256::new();
        let mut buffer = [0; 128 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        Ok(hex(&hasher.finalize()))
    })();
    match result {
        Ok(hash) => Field::Observed(hash),
        Err(error) => Field::error(Errno::from_raw_os_error(
            error.raw_os_error().unwrap_or(libc::EIO),
        )),
    }
}
