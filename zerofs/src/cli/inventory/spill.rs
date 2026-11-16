use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tempfile::{NamedTempFile, TempDir};

pub(super) const MEMORY_BYTES: usize = 1024 * 1024;
pub(super) const MEMORY_ROWS: usize = 4096;
pub(super) const GROUP_ROWS: usize = 256;

pub(super) struct Workspace {
    parent: Option<PathBuf>,
    private: std::cell::OnceCell<TempDir>,
    pub cap: usize,
}

impl Workspace {
    pub fn new(root: &Path, parent: Option<&Path>, cap: usize) -> Result<Rc<Self>> {
        ensure!(cap > 0, "inventory memory cap must be positive");
        let parent = parent.map(std::fs::canonicalize).transpose()?;
        if let Some(parent) = &parent {
            ensure!(
                !parent.starts_with(std::fs::canonicalize(root)?),
                "--work-dir must be outside the source root"
            );
            ensure!(parent.is_dir(), "--work-dir must be an existing directory");
        }
        Ok(Rc::new(Self {
            parent,
            private: std::cell::OnceCell::new(),
            cap,
        }))
    }

    fn file(&self) -> Result<NamedTempFile> {
        if self.private.get().is_none() {
            let parent = self.parent.as_ref().context(
                "inventory memory cap exceeded; provide --work-dir outside the source root",
            )?;
            let directory = tempfile::Builder::new()
                .prefix("zerofs-inventory-")
                .tempdir_in(parent)?;
            self.private
                .set(directory)
                .expect("single-threaded workspace initialization");
        }
        Ok(NamedTempFile::new_in(self.private.get().unwrap().path())?)
    }
}

pub(super) struct Sorter {
    work: Rc<Workspace>,
    buffer: Vec<Vec<u8>>,
    bytes: usize,
    runs: Vec<Option<NamedTempFile>>,
}

impl Sorter {
    pub fn new(work: Rc<Workspace>) -> Self {
        Self {
            work,
            buffer: Vec::new(),
            bytes: 0,
            runs: Vec::new(),
        }
    }

    pub fn push(&mut self, row: Vec<u8>) -> Result<()> {
        let row_bytes = row.capacity();
        let next_capacity = if self.buffer.len() == self.buffer.capacity() {
            self.buffer.capacity().saturating_mul(2).max(4)
        } else {
            self.buffer.capacity()
        };
        let descriptors = next_capacity * std::mem::size_of::<Vec<u8>>();
        if !self.buffer.is_empty()
            && (self.bytes + row_bytes + descriptors > self.work.cap
                || self.buffer.len() == MEMORY_ROWS)
        {
            self.flush()?;
        }
        self.bytes += row_bytes;
        self.buffer.push(row);
        if self.bytes + self.buffer.capacity() * std::mem::size_of::<Vec<u8>>() > self.work.cap {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.buffer.sort_unstable();
        let mut run = self.work.file()?;
        {
            let mut writer = BufWriter::new(run.as_file_mut());
            for row in self.buffer.drain(..) {
                write_row(&mut writer, &row)?;
            }
            writer.flush()?;
        }
        self.buffer = Vec::new();
        self.bytes = 0;
        let mut level = 0;
        loop {
            if level == self.runs.len() {
                self.runs.push(Some(run));
                break;
            }
            if let Some(previous) = self.runs[level].take() {
                run = merge(&self.work, previous, run)?;
                level += 1;
            } else {
                self.runs[level] = Some(run);
                break;
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Rows> {
        if self.runs.is_empty() {
            self.buffer.sort_unstable();
            return Ok(Rows::Memory(self.buffer));
        }
        if !self.buffer.is_empty() {
            self.flush()?;
        }
        let mut runs = self.runs.into_iter().flatten();
        let mut result = runs.next().unwrap();
        for run in runs {
            result = merge(&self.work, result, run)?;
        }
        Ok(Rows::Disk(result, self.work))
    }
}

pub(super) enum Rows {
    Memory(Vec<Vec<u8>>),
    Disk(NamedTempFile, Rc<Workspace>),
}

impl Rows {
    pub fn iter(&self) -> Result<Box<dyn Iterator<Item = Result<Vec<u8>>> + '_>> {
        Ok(match self {
            Self::Memory(rows) => Box::new(rows.iter().cloned().map(Ok)),
            Self::Disk(file, _work) => Box::new(DiskRows(BufReader::new(file.reopen()?))),
        })
    }
}

struct DiskRows(BufReader<File>);
impl Iterator for DiskRows {
    type Item = Result<Vec<u8>>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut size = [0; 8];
        match self.0.read(&mut size[..1]) {
            Ok(0) => return None,
            Ok(_) => {}
            Err(error) => return Some(Err(error.into())),
        }
        Some((|| {
            self.0.read_exact(&mut size[1..])?;
            let mut row = vec![0; usize::try_from(u64::from_le_bytes(size))?];
            self.0.read_exact(&mut row)?;
            Ok(row)
        })())
    }
}

fn write_row(writer: &mut impl Write, row: &[u8]) -> Result<()> {
    writer.write_all(&(row.len() as u64).to_le_bytes())?;
    writer.write_all(row)?;
    Ok(())
}

fn merge(work: &Workspace, left: NamedTempFile, right: NamedTempFile) -> Result<NamedTempFile> {
    let mut left = DiskRows(BufReader::new(left.reopen()?)).peekable();
    let mut right = DiskRows(BufReader::new(right.reopen()?)).peekable();
    let mut output = work.file()?;
    {
        let mut writer = BufWriter::new(output.as_file_mut());
        while left.peek().is_some() || right.peek().is_some() {
            let take_left = match (left.peek(), right.peek()) {
                (Some(Ok(a)), Some(Ok(b))) => a <= b,
                (Some(_), _) => true,
                _ => false,
            };
            let row = if take_left { left.next() } else { right.next() }.unwrap()?;
            write_row(&mut writer, &row)?;
        }
        writer.flush()?;
    }
    Ok(output)
}
