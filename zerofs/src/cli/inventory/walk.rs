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
use std::num::NonZeroU32;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, Instant};

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
    #[cfg(target_os = "linux")]
    root_mount_id: Field<u64>,
    trusted_namespace: Field<()>,
    hash: bool,
    tree_complete: bool,
    links: BTreeMap<(u64, u64), Links>,
    noatime_fallback: bool,
    pacer: Pacer,
}

pub(super) fn inventory(
    root: &Path,
    hash: bool,
    max_entries_per_second: Option<NonZeroU32>,
    writer: impl Write,
) -> Result<bool> {
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
    let mut noatime_fallback = false;
    let root_fd = openat(fs::CWD, path, DIRECTORY_FLAGS, &mut noatime_fallback)
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
    let trusted_namespace = platform::trusted_xattr_namespace();
    let mut entry = stat_entry(
        Vec::new(),
        &before,
        root_fd.as_fd(),
        OsStr::new("."),
        trusted_namespace.scope(),
    );
    output.emit(&Record::Header(Header {
        schema: 3,
        tool_version: env!("CARGO_PKG_VERSION"),
        collector: std::env::consts::OS,
        root: RawBytes::new(raw),
        root_identity: Field::Observed(identity(&before)),
        filesystem,
        trusted_xattr_namespace: trusted_namespace,
        mount_boundary_detection: if cfg!(target_os = "linux") {
            "Device number and STATX_MNT_ID. Missing mount IDs prevent reads and descent."
        } else {
            "Device number only. Mounts retaining the same device number cannot be identified."
        },
        hashing: if hash { "sha256" } else { "not_requested" },
        collision_rules: super::collision::RULES,
        atime_notice: "Times are sampled before opening entries. Linux requests O_NOATIME and retries on EPERM. Symlink reads and fallback opens may update atime. macOS reads may update atime. Only a read-only mount prevents updates. This is not a filesystem snapshot.",
        noatime_requested: cfg!(target_os = "linux"),
        noatime_fallback,
    }))?;
    let mut walker = Walker {
        output: &mut output,
        root_dev: identity(&before).dev,
        #[cfg(target_os = "linux")]
        root_mount_id: entry.mount_id.clone(),
        trusted_namespace: entry.xattrs.trusted_namespace.clone(),
        hash,
        tree_complete: true,
        links: BTreeMap::new(),
        noatime_fallback,
        pacer: Pacer::new(max_entries_per_second),
    };
    walker.pacer.wait();
    if let Some(reason) = walker.boundary_reason(&entry) {
        walker.skip_boundary(entry, reason)?;
    } else {
        walker.collect_fd(&mut entry, root_fd.as_fd(), EntryKind::Directory);
        walker.output.emit(&Record::Entry(Box::new(entry)))?;
        walker.directory(root_fd.as_fd(), &[])?;
        walker.check_directory(root_fd.as_fd(), &[], &before)?;
    }
    walker.finish()?;
    output.finish()
}

impl<W: Write> Walker<'_, W> {
    fn boundary_reason(&self, entry: &Entry) -> Option<Reason> {
        if matches!(entry.identity, Field::Observed(id) if id.dev != self.root_dev) {
            return Some(Reason::MountBoundary);
        }
        #[cfg(target_os = "linux")]
        {
            match (&self.root_mount_id, &entry.mount_id) {
                (Field::Observed(root), Field::Observed(child)) if root == child => None,
                (Field::Observed(_), Field::Observed(_)) => Some(Reason::MountBoundary),
                _ => Some(Reason::MountIdUnavailable),
            }
        }
        #[cfg(target_os = "macos")]
        None
    }

    fn skip_boundary(&mut self, mut entry: Entry, reason: Reason) -> Result<()> {
        self.tree_complete = false;
        let path = entry.path.clone();
        let identity = entry.identity.clone();
        invalidate_reads(&mut entry, reason);
        self.output.emit(&Record::Entry(Box::new(entry)))?;
        self.output.emit(&Record::MountBoundary {
            path,
            identity,
            error: Field::unavailable(reason),
        })
    }

    fn directory(&mut self, fd: BorrowedFd<'_>, path: &[u8]) -> Result<()> {
        let names = match names(fd, &mut self.noatime_fallback) {
            Ok(names) => names,
            Err(error) => {
                self.unreadable(path, error)?;
                return Ok(());
            }
        };
        self.output.non_utf8_names += names
            .iter()
            .filter(|n| std::str::from_utf8(n).is_err())
            .count() as u64;
        for (rule, group) in super::collision::collisions(&names) {
            self.output.emit(&Record::NameCollision(NameCollision {
                dir: RawBytes::new(path),
                rule,
                names: group.iter().map(|n| RawBytes::new(n)).collect(),
            }))?;
        }
        for name in names {
            let child = join(path, &name);
            self.entry(fd, OsStr::from_bytes(&name), child)?;
        }
        Ok(())
    }

    fn entry(&mut self, parent: BorrowedFd<'_>, name: &OsStr, path: Vec<u8>) -> Result<()> {
        self.pacer.wait();
        let before = match fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(error) => {
                let entry = failed_entry(&path, error, self.trusted_namespace.clone());
                self.tree_complete &= entry.kind.is_complete();
                self.output.emit(&Record::Entry(Box::new(entry)))?;
                return Ok(());
            }
        };
        let entry_kind = kind(&before);
        let mut entry = stat_entry(
            path.clone(),
            &before,
            parent,
            name,
            self.trusted_namespace.clone(),
        );
        if let Some(reason) = self.boundary_reason(&entry) {
            return self.skip_boundary(entry, reason);
        }
        let fd = match entry_kind {
            EntryKind::Directory => Some(openat(
                parent,
                name,
                DIRECTORY_FLAGS,
                &mut self.noatime_fallback,
            )),
            EntryKind::File => Some(open_regular_file(
                parent,
                name,
                &before,
                &entry,
                &mut self.noatime_fallback,
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
                Ok(opened)
                    if same_version(&before, &opened)
                        && same_mount(&entry, fd.as_fd(), OsStr::new(""), &opened) =>
                {
                    Some(fd)
                }
                Ok(_) => {
                    self.changed(&path)?;
                    invalidate_reads(&mut entry, Reason::EntryChanged);
                    self.output.emit(&Record::Entry(Box::new(entry)))?;
                    return Ok(());
                }
                Err(error) => {
                    self.open_failed(&mut entry, entry_kind, &path, error)?;
                    None
                }
            },
            Some(Err(Errno::STALE)) => {
                self.changed(&path)?;
                invalidate_reads(&mut entry, Reason::EntryChanged);
                self.output.emit(&Record::Entry(Box::new(entry)))?;
                return Ok(());
            }
            Some(Err(error)) => {
                self.open_failed(&mut entry, entry_kind, &path, error)?;
                None
            }
            None => None,
        };
        if let Some(fd) = &fd {
            self.collect_fd(&mut entry, fd.as_fd(), entry_kind);
        }
        if !matches!(entry_kind, EntryKind::Directory | EntryKind::File) {
            #[cfg(target_os = "linux")]
            {
                (entry.xattrs.listing, entry.acl) =
                    platform::path_metadata(parent, name, entry_kind);
            }
            #[cfg(target_os = "macos")]
            if entry_kind != EntryKind::Symlink {
                entry.xattrs.listing = Field::unavailable(Reason::NotImplementedOnPlatform);
                entry.acl = Field::unavailable(Reason::NotImplementedOnPlatform);
            }
        }
        if entry_kind == EntryKind::Symlink {
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
        let stable = matches!(&after, Ok(after) if same_version(&before, after)
        && if let Some(fd) = &fd {
            same_mount(&entry, fd.as_fd(), OsStr::new(""), after)
        } else {
            same_mount(&entry, parent, name, after)
        }) && (entry_kind != EntryKind::Symlink
            || matches!(fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW), Ok(after) if same_version(&before, &after)));
        if !stable {
            self.changed(&path)?;
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
            self.check_directory(fd.as_fd(), &path, &before)?;
        }
        Ok(())
    }

    fn collect_fd(&self, entry: &mut Entry, fd: BorrowedFd<'_>, kind: EntryKind) {
        entry.xattrs.listing = collect::xattrs(fd);
        entry.acl = platform::acl(fd);
        #[cfg(target_os = "linux")]
        {
            entry.platform_flags = platform::fd_flags(fd);
        }
        if kind == EntryKind::File {
            if let Field::Observed(size) = entry.size {
                entry.data_extents = extents(fd, size);
            }
            if self.hash {
                entry.content_sha256 = content_hash(fd);
            }
        }
    }

    fn open_failed(
        &mut self,
        entry: &mut Entry,
        kind: EntryKind,
        path: &[u8],
        error: Errno,
    ) -> Result<()> {
        entry.xattrs.listing = Field::error(error);
        entry.acl = Field::error(error);
        #[cfg(target_os = "linux")]
        {
            entry.platform_flags = Field::error(error);
        }
        if kind == EntryKind::File {
            entry.data_extents = Field::error(error);
            if self.hash {
                entry.content_sha256 = Field::error(error);
            }
        }
        if kind == EntryKind::Directory {
            self.unreadable(path, error)?;
        }
        Ok(())
    }

    fn unreadable(&mut self, path: &[u8], error: Errno) -> Result<()> {
        let error = Field::error(error);
        self.tree_complete &= error.is_complete();
        self.output.emit(&Record::DirectoryUnreadable {
            path: RawBytes::new(path),
            error,
        })
    }

    fn changed(&mut self, path: &[u8]) -> Result<()> {
        self.tree_complete &= Reason::EntryChanged.counts_as_complete();
        self.output.emit(&Record::EntryChanged {
            path: RawBytes::new(path),
            error: Field::unavailable(Reason::EntryChanged),
        })
    }

    fn check_directory(&mut self, fd: BorrowedFd<'_>, path: &[u8], before: &Stat) -> Result<()> {
        if !matches!(fs::fstat(fd), Ok(after) if same_version(before, &after)) {
            self.changed(path)?;
        }
        Ok(())
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
        self.output.emit(&Record::HeaderUpdate {
            noatime_fallback: self.noatime_fallback,
        })
    }
}

struct Pacer {
    interval: Option<Duration>,
    previous: Option<Instant>,
}

impl Pacer {
    fn new(rate: Option<NonZeroU32>) -> Self {
        Self {
            interval: rate
                .map(|n| Duration::from_nanos(1_000_000_000_u64.div_ceil(n.get().into()))),
            previous: None,
        }
    }

    fn wait(&mut self) {
        if let Some(interval) = self.interval {
            if let Some(previous) = self.previous {
                std::thread::sleep(interval.saturating_sub(previous.elapsed()));
            }
            self.previous = Some(Instant::now());
        }
    }
}

fn openat(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    flags: OFlags,
    noatime_fallback: &mut bool,
) -> Result<OwnedFd, Errno> {
    #[cfg(target_os = "linux")]
    match fs::openat(parent, name, flags | OFlags::NOATIME, Mode::empty()) {
        Err(Errno::PERM) => *noatime_fallback = true,
        result => return result,
    }
    #[cfg(target_os = "macos")]
    let _ = noatime_fallback;
    fs::openat(parent, name, flags, Mode::empty())
}

fn open_regular_file(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    before: &Stat,
    entry: &Entry,
    noatime_fallback: &mut bool,
) -> Result<OwnedFd, Errno> {
    #[cfg(target_os = "linux")]
    {
        let pinned = pin_regular_file(parent, name, before, entry)?;
        read_pinned_file(pinned.as_fd(), noatime_fallback)
    }
    #[cfg(target_os = "macos")]
    {
        let _ = (before, entry);
        openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            noatime_fallback,
        )
    }
}

#[cfg(target_os = "linux")]
fn pin_regular_file(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    before: &Stat,
    entry: &Entry,
) -> Result<OwnedFd, Errno> {
    // O_NONBLOCK does not make opening a device harmless. O_PATH pins the
    // entry without opening it for I/O, so a swap after stat cannot cause the
    // collector to open a FIFO or device before discovering its new type.
    let pinned = fs::openat(
        parent,
        name,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let stat = fs::fstat(&pinned)?;
    if kind(&stat) != EntryKind::File
        || !same_version(before, &stat)
        || !same_mount(entry, pinned.as_fd(), OsStr::new(""), &stat)
    {
        return Err(Errno::STALE);
    }
    Ok(pinned)
}

#[cfg(target_os = "linux")]
fn read_pinned_file(pinned: BorrowedFd<'_>, noatime_fallback: &mut bool) -> Result<OwnedFd, Errno> {
    use rustix::fd::AsRawFd;
    let path = format!("/proc/self/fd/{}", pinned.as_raw_fd());
    // Follow only this process's descriptor link, not the mutable source name.
    // The O_PATH descriptor remains live until the data descriptor is opened.
    openat(
        fs::CWD,
        OsStr::new(&path),
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        noatime_fallback,
    )
}

fn names(fd: BorrowedFd<'_>, noatime_fallback: &mut bool) -> Result<Vec<Vec<u8>>, Errno> {
    let scan = openat(fd, OsStr::new("."), DIRECTORY_FLAGS, noatime_fallback)?;
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
fn stat_entry(
    path: Vec<u8>,
    stat: &Stat,
    parent: BorrowedFd<'_>,
    name: &OsStr,
    trusted_namespace: Field<()>,
) -> Entry {
    Entry {
        path: RawBytes::new(&path),
        kind: Field::Observed(kind(stat)),
        identity: Field::Observed(identity(stat)),
        #[cfg(target_os = "linux")]
        mount_id: platform::mount_id(parent, name, stat),
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
        xattrs: Xattrs {
            listing: Field::unavailable(Reason::NotCollectedForKind),
            trusted_namespace,
        },
        acl: Field::unavailable(Reason::NotCollectedForKind),
        content_sha256: Field::unavailable(if kind(stat) == EntryKind::File {
            Reason::NotRequested
        } else {
            Reason::NotCollectedForKind
        }),
    }
}

fn failed_entry(path: &[u8], error: Errno, trusted_namespace: Field<()>) -> Entry {
    Entry {
        path: RawBytes::new(path),
        kind: Field::error(error),
        identity: Field::error(error),
        #[cfg(target_os = "linux")]
        mount_id: Field::error(error),
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
        xattrs: Xattrs {
            listing: Field::error(error),
            trusted_namespace,
        },
        acl: Field::error(error),
        content_sha256: Field::error(error),
    }
}

fn invalidate_reads(entry: &mut Entry, reason: Reason) {
    #[cfg(target_os = "linux")]
    if matches!(
        entry.kind,
        Field::Observed(EntryKind::File | EntryKind::Directory)
    ) {
        entry.platform_flags = Field::unavailable(reason);
    }
    entry.xattrs.listing = Field::unavailable(reason);
    entry.acl = Field::unavailable(reason);
    entry.data_extents = Field::unavailable(reason);
    entry.symlink_target = Field::unavailable(reason);
    entry.content_sha256 = Field::unavailable(reason);
}

#[cfg(target_os = "linux")]
fn same_mount(entry: &Entry, parent: BorrowedFd<'_>, name: &OsStr, stat: &Stat) -> bool {
    platform::mount_id(parent, name, stat) == entry.mount_id
}

#[cfg(target_os = "macos")]
fn same_mount(_entry: &Entry, _parent: BorrowedFd<'_>, _name: &OsStr, _stat: &Stat) -> bool {
    true
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn observed_file(root: &Path) -> (OwnedFd, Stat, Entry) {
        std::fs::write(root.join("file"), b"original contents").unwrap();
        let parent = fs::open(root, DIRECTORY_FLAGS, Mode::empty()).unwrap();
        let before = fs::statat(&parent, "file", AtFlags::SYMLINK_NOFOLLOW).unwrap();
        let entry = stat_entry(
            b"file".to_vec(),
            &before,
            parent.as_fd(),
            OsStr::new("file"),
            Field::Observed(()),
        );
        (parent, before, entry)
    }

    fn replace_with_fifo(root: &Path) {
        std::fs::rename(root.join("file"), root.join("retained")).unwrap();
        assert!(
            std::process::Command::new("mkfifo")
                .arg(root.join("file"))
                .status()
                .unwrap()
                .success()
        );
    }

    #[test]
    fn regular_file_replaced_before_pin_is_not_opened_for_io() {
        let root = tempfile::tempdir().unwrap();
        let (parent, before, entry) = observed_file(root.path());
        replace_with_fifo(root.path());
        let mut noatime_fallback = false;
        assert_eq!(
            open_regular_file(
                parent.as_fd(),
                OsStr::new("file"),
                &before,
                &entry,
                &mut noatime_fallback,
            )
            .unwrap_err(),
            Errno::STALE
        );
    }

    #[test]
    fn regular_file_replaced_after_pin_opens_the_retained_inode() {
        let root = tempfile::tempdir().unwrap();
        let (parent, before, entry) = observed_file(root.path());
        let pinned = pin_regular_file(parent.as_fd(), OsStr::new("file"), &before, &entry).unwrap();
        replace_with_fifo(root.path());
        let mut noatime_fallback = false;
        let read = read_pinned_file(pinned.as_fd(), &mut noatime_fallback).unwrap();
        let mut file = std::fs::File::from(read);
        assert!(file.metadata().unwrap().is_file());
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "original contents");
    }
}
