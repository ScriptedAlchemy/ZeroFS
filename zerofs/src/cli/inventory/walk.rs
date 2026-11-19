use super::Manifest;
use super::collect;
use super::platform;
use super::record::*;
use super::resume::{self, Checkpoint, Frame, Version};
use super::spill::{GROUP_ROWS, Rows, Sorter, Workspace};
use anyhow::{Context, Result, bail};
use rustix::fd::{AsFd, BorrowedFd, OwnedFd};
use rustix::fs::{self, AtFlags, Dir, Mode, OFlags, SeekFrom, Stat};
use rustix::io::Errno;
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::io::{BufRead, Read, Write};
use std::num::NonZeroU32;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

pub(super) struct Options<'a> {
    pub work_dir: Option<&'a Path>,
    pub resume_from: Option<&'a Path>,
    pub memory_bytes: usize,
}

impl Default for Options<'_> {
    fn default() -> Self {
        Self {
            work_dir: None,
            resume_from: None,
            memory_bytes: super::spill::MEMORY_BYTES,
        }
    }
}

struct Walker<'a, W> {
    output: &'a mut Manifest<W>,
    root_dev: u64,
    #[cfg(target_os = "linux")]
    root_mount_id: Field<u64>,
    #[cfg(target_os = "macos")]
    root_mount: Field<MountIdentity>,
    trusted_namespace: Field<()>,
    hash: bool,
    tree_complete: bool,
    links: Sorter,
    work: Rc<Workspace>,
    frames: Vec<Frame>,
    noatime_fallback: bool,
    pacer: Pacer,
}

#[cfg(test)]
pub(super) fn inventory(
    root: &Path,
    hash: bool,
    max_entries_per_second: Option<NonZeroU32>,
    writer: impl Write,
) -> Result<bool> {
    inventory_with_options(
        root,
        hash,
        max_entries_per_second,
        Options::default(),
        writer,
    )
}

pub(super) fn inventory_with_options(
    root: &Path,
    hash: bool,
    max_entries_per_second: Option<NonZeroU32>,
    options: Options<'_>,
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
    #[cfg(target_os = "linux")]
    let pinned_root = platform::proc_fd_path(&root_fd);
    #[cfg(target_os = "linux")]
    let root = pinned_root.as_path();
    let work = Workspace::new(root, options.work_dir, options.memory_bytes)?;
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
    let header = Record::Header(Header {
        schema: 5,
        tool_version: env!("CARGO_PKG_VERSION"),
        collector: std::env::consts::OS,
        root: RawBytes::new(raw),
        root_identity: Field::Observed(identity(&before)),
        filesystem,
        trusted_xattr_namespace: trusted_namespace,
        mount_boundary_detection: if cfg!(target_os = "linux") {
            "Device number and STATX_MNT_ID. Missing mount IDs prevent reads and descent."
        } else {
            "Device number, then the fstatfs filesystem ID and mount point of each opened entry. Missing mount identities prevent reads and descent."
        },
        hashing: if hash { "sha256" } else { "not_requested" },
        collision_rules: super::collision::RULES,
        max_entries_per_second: max_entries_per_second.map(NonZeroU32::get),
        atime_notice: "Times are sampled before opening entries. Linux requests O_NOATIME and retries on EPERM. Symlink reads and fallback opens may update atime. macOS reads may update atime. Only a read-only mount prevents updates. This is not a filesystem snapshot.",
        noatime_requested: cfg!(target_os = "linux"),
        noatime_fallback,
    });
    let resume = options
        .resume_from
        .map(|path| resume::Resume::open(path, &serde_json::to_value(&header)?))
        .transpose()?;
    if resume.is_none() {
        output.emit(&header)?;
    }
    let mut walker = Walker {
        output: &mut output,
        root_dev: identity(&before).dev,
        #[cfg(target_os = "linux")]
        root_mount_id: entry.mount_id.clone(),
        #[cfg(target_os = "macos")]
        root_mount: platform::mount_identity(root_fd.as_fd()),
        trusted_namespace: entry.xattrs.trusted_namespace.clone(),
        hash,
        tree_complete: true,
        links: Sorter::new(work.clone()),
        work,
        frames: Vec::new(),
        noatime_fallback,
        pacer: Pacer::new(max_entries_per_second),
    };
    if let Some(mut resume) = resume {
        let mut copied = 0;
        let mut line = Vec::new();
        while copied < resume.end {
            line.clear();
            let count = resume.reader.read_until(b'\n', &mut line)?;
            anyhow::ensure!(count > 0, "resume manifest changed while replaying");
            copied += count as u64;
            anyhow::ensure!(
                copied <= resume.end,
                "resume manifest changed while replaying"
            );
            let value: serde_json::Value = serde_json::from_slice(&line)?;
            if value["record"] == "hardlink_observation" {
                let link: LinkObservation = serde_json::from_value(value.clone())?;
                walker.links.push(link_row(&link)?)?;
            }
            walker.output.replay(&value, &line)?;
        }
        anyhow::ensure!(
            hex(&walker.output.hash.clone().finalize()) == resume.digest,
            "resume manifest changed while replaying"
        );
        walker.tree_complete = resume.checkpoint.tree_complete;
        walker.noatime_fallback |= resume.checkpoint.noatime_fallback;
        if !resume.checkpoint.ancestors.is_empty() {
            walker.resume_directory(root_fd.as_fd(), &resume.checkpoint.ancestors)?;
        }
    } else {
        walker.pacer.wait();
        if let Some(reason) = walker
            .boundary_reason(&entry)
            .or_else(|| walker.descriptor_boundary(root_fd.as_fd()))
        {
            walker.skip_boundary(entry, reason)?;
        } else {
            walker.collect_fd(&mut entry, root_fd.as_fd(), EntryKind::Directory);
            translate_nfs4_acls(&mut entry);
            walker.output.emit(&Record::Entry(Box::new(entry)))?;
            walker.directory(root_fd.as_fd(), &[], &before)?;
        }
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
            mount_boundary(&self.root_mount_id, &entry.mount_id)
        }
        #[cfg(target_os = "macos")]
        None
    }

    /// Checks an opened entry before any read through it. macOS reports a mount identity only
    /// for an open descriptor. Linux has already compared statx mount IDs before opening.
    fn descriptor_boundary(&self, fd: BorrowedFd<'_>) -> Option<Reason> {
        #[cfg(target_os = "macos")]
        {
            mount_boundary(&self.root_mount, &platform::mount_identity(fd))
        }
        #[cfg(target_os = "linux")]
        {
            let _ = fd;
            None
        }
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

    fn directory(&mut self, fd: BorrowedFd<'_>, path: &[u8], before: &Stat) -> Result<()> {
        anyhow::ensure!(
            self.frames.len() < 128,
            "inventory directory depth exceeds 128"
        );
        self.frames.push(Frame {
            path: RawBytes::new(path),
            version: version(before),
            mount_id: mount_number(fd, before),
            last_child: None,
        });
        self.directory_contents(fd, path, None)?;
        self.complete_directory(fd)
    }

    fn directory_contents(
        &mut self,
        fd: BorrowedFd<'_>,
        path: &[u8],
        after: Option<&[u8]>,
    ) -> Result<()> {
        let (names, folded) = match names(fd, &mut self.noatime_fallback, self.work.clone()) {
            Ok(names) => names,
            Err(error) => {
                if let Some(error) = error.downcast_ref::<Errno>() {
                    self.unreadable(path, *error)?;
                    return Ok(());
                }
                return Err(error);
            }
        };
        if after.is_none() {
            super::collision::emit(&folded, path, self.output)?;
        }
        drop(folded);
        for name in names.iter()? {
            let name = name?;
            if after.is_some_and(|after| name.as_slice() <= after) {
                continue;
            }
            self.frames.last_mut().unwrap().last_child = Some(RawBytes::new(&name));
            let child = join(path, &name);
            self.entry(fd, OsStr::from_bytes(&name), child)?;
        }
        Ok(())
    }

    fn complete_directory(&mut self, fd: BorrowedFd<'_>) -> Result<()> {
        let frame = self.frames.pop().unwrap();
        let path = resume::bytes(&frame.path)?;
        if !matches!(fs::fstat(fd), Ok(after) if version(&after) == frame.version) {
            self.changed(&path)?;
        }
        let mut checkpoint = Checkpoint {
            path: frame.path,
            ancestors: self.frames.clone(),
            tree_complete: self.tree_complete,
            noatime_fallback: self.noatime_fallback,
            prefix_sha256: hex(&self.output.hash.clone().finalize()),
            state_sha256: String::new(),
        };
        checkpoint.state_sha256 = checkpoint.state_digest()?;
        self.output.emit(&Record::DirectoryComplete(checkpoint))?;
        // A flushed checkpoint is usable even when the next output write fails.
        self.output.writer.flush()?;
        Ok(())
    }

    fn resume_directory(&mut self, fd: BorrowedFd<'_>, frames: &[Frame]) -> Result<()> {
        let frame = &frames[0];
        let stat = fs::fstat(fd)?;
        anyhow::ensure!(
            version(&stat) == frame.version && mount_number(fd, &stat) == frame.mount_id,
            "resume ancestor directory changed"
        );
        // Resumed ancestors bypass `entry`, so they must repeat its mount-boundary check
        // before enumerating names. Device/inode identity alone cannot identify an overmount.
        #[cfg(target_os = "linux")]
        let boundary = mount_boundary(
            &self.root_mount_id,
            &platform::mount_id(fd, OsStr::new(""), &stat),
        );
        #[cfg(target_os = "macos")]
        let boundary = self.descriptor_boundary(fd);
        anyhow::ensure!(
            boundary.is_none(),
            "resume ancestor mount identity is unavailable or outside the inventory root"
        );
        let path = resume::bytes(&frame.path)?;
        self.frames.push(frame.clone());
        let last = frame.last_child.as_ref().map(resume::bytes).transpose()?;
        if frames.len() > 1 {
            let name = last
                .as_deref()
                .context("resume ancestor has no child cursor")?;
            anyhow::ensure!(
                join(&path, name) == resume::bytes(&frames[1].path)?,
                "invalid resume ancestor chain"
            );
            let child = openat(
                fd,
                OsStr::from_bytes(name),
                DIRECTORY_FLAGS,
                &mut self.noatime_fallback,
            )?;
            self.resume_directory(child.as_fd(), &frames[1..])?;
        }
        self.directory_contents(fd, &path, last.as_deref())?;
        self.complete_directory(fd)
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
        // Entries without a descriptor are FIFOs, sockets and devices. macOS mounts only on
        // directories, so these share the mount of the directory that was already checked.
        if let Some(fd) = &fd
            && let Some(reason) = self.descriptor_boundary(fd.as_fd())
        {
            return self.skip_boundary(entry, reason);
        }
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
            let link = LinkObservation {
                dev: id.dev,
                ino: id.ino,
                nlink: before.st_nlink as u64,
                stable,
                path: RawBytes::new(&path),
            };
            self.links.push(link_row(&link)?)?;
            self.output.emit(&Record::HardlinkObservation(link))?;
        }
        translate_nfs4_acls(&mut entry);
        self.output.emit(&Record::Entry(Box::new(entry)))?;
        if stable
            && entry_kind == EntryKind::Directory
            && let Some(fd) = fd
        {
            self.directory(fd.as_fd(), &path, &before)?;
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

    fn finish(self) -> Result<()> {
        let rows = self.links.finish()?;
        let mut metadata = rows.iter()?.peekable();
        let mut paths = rows.iter()?;
        while let Some(first) = metadata.next() {
            let first = first?;
            let dev = u64::from_be_bytes(first[..8].try_into()?);
            let ino = u64::from_be_bytes(first[8..16].try_into()?);
            let (nlink, stable) = link_metadata(&first);
            let mut consistent = stable;
            let mut count = 1_u64;
            while let Some(row) = metadata.next_if(|row| match row {
                Ok(row) => row[..16] == first[..16],
                Err(_) => true,
            }) {
                let row = row?;
                let (other_nlink, stable) = link_metadata(&row);
                consistent &= stable && other_nlink == nlink;
                count += 1;
            }
            let observed_nlink = if consistent && nlink >= count {
                Field::Observed(nlink)
            } else {
                Field::unavailable(Reason::EntryChanged)
            };
            let outside = if self.tree_complete && consistent && nlink >= count {
                Field::Observed(nlink - count)
            } else {
                Field::unavailable(Reason::IncompleteTraversal)
            };
            let mut remaining = count;
            let mut chunk = 0;
            while remaining > 0 {
                let take = remaining.min(GROUP_ROWS as u64);
                let mut group = Vec::new();
                for _ in 0..take {
                    let row = paths.next().unwrap()?;
                    group.push(RawBytes::new(&row[16..row.len() - 10]));
                }
                remaining -= take;
                self.output.emit(&Record::HardlinkGroup(HardlinkGroup {
                    dev,
                    ino,
                    nlink: observed_nlink.clone(),
                    paths_in_tree: group,
                    links_outside_tree: outside.clone(),
                    chunk,
                    last_chunk: remaining == 0,
                }))?;
                chunk += 1;
            }
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

fn names(
    fd: BorrowedFd<'_>,
    noatime_fallback: &mut bool,
    work: Rc<Workspace>,
) -> Result<(Rows, Rows)> {
    let scan = openat(fd, OsStr::new("."), DIRECTORY_FLAGS, noatime_fallback)?;
    let directory = Dir::new(scan)?;
    let mut names = Sorter::new(work.clone());
    let mut folded = Sorter::new(work);
    for entry in directory {
        let entry = entry?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec())?;
            super::collision::push(&mut folded, name)?;
        }
    }
    Ok((names.finish()?, folded.finish()?))
}

fn link_row(link: &LinkObservation) -> Result<Vec<u8>> {
    let mut row = link.dev.to_be_bytes().to_vec();
    row.extend_from_slice(&link.ino.to_be_bytes());
    row.extend_from_slice(&resume::bytes(&link.path)?);
    row.push(0);
    row.extend_from_slice(&link.nlink.to_be_bytes());
    row.push(u8::from(link.stable));
    Ok(row)
}

fn link_metadata(row: &[u8]) -> (u64, bool) {
    (
        u64::from_be_bytes(row[row.len() - 9..row.len() - 1].try_into().unwrap()),
        row[row.len() - 1] != 0,
    )
}

#[allow(clippy::unnecessary_cast)]
fn version(stat: &Stat) -> Version {
    Version {
        dev: stat.st_dev as u64,
        ino: stat.st_ino as u64,
        mode: stat.st_mode as u32,
        nlink: stat.st_nlink as u64,
        uid: stat.st_uid,
        gid: stat.st_gid,
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        rdev: stat.st_rdev as u64,
        mtime: (stat.st_mtime as i64, stat.st_mtime_nsec as i64),
        ctime: (stat.st_ctime as i64, stat.st_ctime_nsec as i64),
    }
}

fn mount_number(fd: BorrowedFd<'_>, stat: &Stat) -> Option<u64> {
    #[cfg(target_os = "linux")]
    if let Field::Observed(id) = platform::mount_id(fd, OsStr::new(""), stat) {
        return Some(id);
    }
    #[cfg(target_os = "macos")]
    let _ = (fd, stat);
    None
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

/// Run the engine's NFSv4-to-POSIX translation on each observed NFSv4 ACL,
/// so the inventory says per entry whether ZeroFS would enforce it.
pub(super) fn translate_nfs4_acls(entry: &mut Entry) {
    let (Field::Observed(kind), Field::Observed(owner), Field::Observed(acls)) =
        (&entry.kind, &entry.owner, &mut entry.acl)
    else {
        return;
    };
    for acl in acls {
        if acl.format != "nfs4-xdr-xattr" {
            continue;
        }
        let Field::Observed(Some(raw)) = &acl.raw else {
            continue;
        };
        let Ok(bytes) = resume::decode_hex(&raw.hex) else {
            continue;
        };
        acl.posix_translation = Some(
            match crate::fs::permissions::nfs4::map_nfs4_acl(
                &bytes,
                *kind == EntryKind::Directory,
                owner.uid,
                owner.gid,
            ) {
                Ok(mapped) => PosixTranslation::Accepted {
                    access: mapped.access.to_text(),
                    default: mapped.default.as_ref().map(|d| d.to_text()),
                    narrowed: mapped.reports.into_iter().collect(),
                },
                Err(refusal) => PosixTranslation::Refused {
                    rule: refusal.to_string(),
                },
            },
        );
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

// macOS names a mount only from a descriptor. `descriptor_boundary` checks it before any read.
#[cfg(target_os = "macos")]
fn same_mount(_entry: &Entry, _parent: BorrowedFd<'_>, _name: &OsStr, _stat: &Stat) -> bool {
    true
}

// Reading the source may update its access time, so the comparison excludes it.
fn same_version(a: &Stat, b: &Stat) -> bool {
    version(a) == version(b)
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

/// The filesystem identity that macOS `fstatfs` reports for an open descriptor.
#[cfg(any(target_os = "macos", test))]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct MountIdentity {
    pub fsid: [i32; 2],
    pub mount_point: Vec<u8>,
}

/// Compares an entry's mount identity with the root's. Linux passes statx mount IDs and macOS
/// passes `fstatfs` identities. A missing identity on either side prevents reads and descent.
fn mount_boundary<T: PartialEq>(root: &Field<T>, entry: &Field<T>) -> Option<Reason> {
    match (root, entry) {
        (Field::Observed(root), Field::Observed(entry)) if root == entry => None,
        (Field::Observed(_), Field::Observed(_)) => Some(Reason::MountBoundary),
        _ => Some(Reason::MountIdUnavailable),
    }
}

#[cfg(test)]
mod mount_boundary_tests {
    use super::*;

    fn mount(fsid: [i32; 2], mount_point: &str) -> Field<MountIdentity> {
        Field::Observed(MountIdentity {
            fsid,
            mount_point: mount_point.as_bytes().to_vec(),
        })
    }

    fn unreadable() -> Field<MountIdentity> {
        Field::Unavailable {
            reason: Reason::MountIdUnavailable,
            errno: Some(libc::EIO),
        }
    }

    #[test]
    fn identical_mount_identity_is_the_same_mount() {
        let root = mount([7, 26], "/Volumes/source");
        assert_eq!(
            mount_boundary(&root, &mount([7, 26], "/Volumes/source")),
            None
        );
    }

    #[test]
    fn another_mount_point_with_the_same_filesystem_id_is_a_boundary() {
        // The walker reaches this comparison only when the device numbers are equal.
        let root = mount([7, 26], "/Volumes/source");
        assert_eq!(
            mount_boundary(&root, &mount([7, 26], "/Volumes/source/nested")),
            Some(Reason::MountBoundary)
        );
    }

    #[test]
    fn another_filesystem_id_on_the_same_mount_point_is_a_boundary() {
        let root = mount([7, 26], "/Volumes/source");
        assert_eq!(
            mount_boundary(&root, &mount([8, 26], "/Volumes/source")),
            Some(Reason::MountBoundary)
        );
    }

    #[test]
    fn unreadable_mount_identity_prevents_reads_and_descent() {
        let root = mount([7, 26], "/Volumes/source");
        assert_eq!(
            mount_boundary(&root, &unreadable()),
            Some(Reason::MountIdUnavailable)
        );
        assert_eq!(
            mount_boundary(&unreadable(), &root),
            Some(Reason::MountIdUnavailable)
        );
    }

    #[test]
    fn resume_refuses_foreign_or_unavailable_mount_before_reading_directory() {
        let _guard = super::super::tests::FIXTURE_LOCK.lock().unwrap();
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("must-not-read"), b"contents").unwrap();
        let fd = fs::open(root.path(), DIRECTORY_FLAGS, Mode::empty()).unwrap();
        let stat = fs::fstat(&fd).unwrap();
        for missing in [false, true] {
            let mut output = Manifest::new(Vec::new());
            let work =
                Workspace::new(root.path(), None, super::super::spill::MEMORY_BYTES).unwrap();
            let mut walker = Walker {
                output: &mut output,
                root_dev: identity(&stat).dev,
                #[cfg(target_os = "linux")]
                root_mount_id: if missing {
                    Field::unavailable(Reason::MountIdUnavailable)
                } else {
                    Field::Observed(mount_number(fd.as_fd(), &stat).unwrap().wrapping_add(1))
                },
                #[cfg(target_os = "macos")]
                root_mount: if missing {
                    unreadable()
                } else {
                    mount([0, 0], "/not-this-mount")
                },
                trusted_namespace: Field::Observed(()),
                hash: true,
                tree_complete: true,
                links: Sorter::new(work.clone()),
                work,
                frames: Vec::new(),
                noatime_fallback: false,
                pacer: Pacer::new(None),
            };
            let frame = Frame {
                path: RawBytes::new(b""),
                version: version(&stat),
                mount_id: mount_number(fd.as_fd(), &stat),
                last_child: None,
            };
            let error = walker
                .resume_directory(fd.as_fd(), &[frame])
                .expect_err("resume must enforce the root mount boundary before descent");
            assert!(error.to_string().contains("mount identity"), "{error}");
            assert!(walker.frames.is_empty());
            assert_eq!(walker.output.entries, 0);
        }
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
        let _guard = super::super::tests::FIXTURE_LOCK.lock().unwrap();
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
        let _guard = super::super::tests::FIXTURE_LOCK.lock().unwrap();
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

#[cfg(test)]
mod nfs4_translation_tests {
    use super::*;
    use crate::fs::permissions::nfs4::encode::{self, Who};

    fn entry_with_nfs4(kind: EntryKind, raw: Option<Vec<u8>>) -> Entry {
        let mut entry = failed_entry(b"share", Errno::INVAL, Field::Observed(()));
        entry.kind = Field::Observed(kind);
        entry.owner = Field::Observed(Owner {
            uid: 1000,
            gid: 100,
        });
        entry.acl = Field::Observed(vec![
            RawAcl {
                format: "linux-posix-access-xattr",
                raw: Field::Observed(None),
                decoded: Field::unavailable(Reason::NotDecoded),
                posix_translation: None,
            },
            RawAcl {
                format: "nfs4-xdr-xattr",
                raw: Field::Observed(raw.as_deref().map(RawBytes::new)),
                decoded: Field::unavailable(Reason::NotDecoded),
                posix_translation: None,
            },
        ]);
        entry
    }

    #[test]
    fn entries_report_the_engine_translation_and_summary_counts_formats() {
        let restricted = encode::acl(&[
            (encode::ALLOW, encode::FD, encode::FULL_CONTROL, Who::Owner),
            (
                encode::ALLOW,
                encode::FD,
                encode::MODIFY | encode::DELETE_CHILD,
                Who::Group,
            ),
        ]);
        let refused = encode::acl(&[(
            encode::ALLOW,
            encode::FILE_INHERIT,
            encode::FULL_CONTROL,
            Who::Owner,
        )]);
        let mut accepted = entry_with_nfs4(EntryKind::Directory, Some(restricted));
        translate_nfs4_acls(&mut accepted);
        let mut rejected = entry_with_nfs4(EntryKind::Directory, Some(refused));
        translate_nfs4_acls(&mut rejected);
        let mut absent = entry_with_nfs4(EntryKind::File, None);
        translate_nfs4_acls(&mut absent);

        let a = serde_json::to_value(&accepted).unwrap();
        let t = &a["acl"]["value"][1]["posix_translation"];
        assert_eq!(t["status"], "accepted");
        assert_eq!(t["access"], "u::rwx,g::rwx,o::---");
        assert_eq!(t["default"], "u::rwx,g::rwx,o::---");
        assert_eq!(
            t["narrowed"],
            serde_json::json!(["delete_follows_parent_directory_write"])
        );
        assert!(a["acl"]["value"][0].get("posix_translation").is_none());
        let r = serde_json::to_value(&rejected).unwrap();
        assert_eq!(
            r["acl"]["value"][1]["posix_translation"],
            serde_json::json!({"status": "refused", "rule": "file_or_directory_only_inheritance"})
        );
        let n = serde_json::to_value(&absent).unwrap();
        assert!(n["acl"]["value"][1].get("posix_translation").is_none());

        let mut out = Vec::new();
        let mut manifest = Manifest::new(&mut out);
        for entry in [accepted, rejected, absent] {
            manifest.emit(&Record::Entry(Box::new(entry))).unwrap();
        }
        manifest.finish().unwrap();
        let summary = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .find(|r| r["record"] == "summary")
            .unwrap();
        assert_eq!(
            summary["acl_formats"],
            serde_json::json!({"nfs4-xdr-xattr": 2})
        );
        assert_eq!(summary["nfs4_acl_translations_accepted"], 1);
        assert_eq!(summary["nfs4_acl_translations_refused"], 1);
    }
}
