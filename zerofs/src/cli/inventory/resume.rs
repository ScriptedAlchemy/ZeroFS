use super::record::RawBytes;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Version {
    pub dev: u64,
    pub ino: u64,
    pub mode: u32,
    pub nlink: u64,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub blocks: u64,
    pub rdev: u64,
    pub mtime: (i64, i64),
    pub ctime: (i64, i64),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Frame {
    pub path: RawBytes,
    pub version: Version,
    pub mount_id: Option<u64>,
    pub last_child: Option<RawBytes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub path: RawBytes,
    pub ancestors: Vec<Frame>,
    pub tree_complete: bool,
    pub noatime_fallback: bool,
    pub prefix_sha256: String,
    pub state_sha256: String,
}

impl Checkpoint {
    pub(super) fn state_digest(&self) -> Result<String> {
        // The prefix digest cannot protect the final checkpoint itself. Cover
        // every cursor and traversal field before trusting it to skip entries.
        // The tuple order is part of manifest schema 5.
        let state = serde_json::to_vec(&(
            &self.path,
            &self.ancestors,
            self.tree_complete,
            self.noatime_fallback,
            &self.prefix_sha256,
        ))?;
        Ok(super::record::hex(&Sha256::digest(state)))
    }
}

pub(super) struct Resume {
    pub reader: BufReader<File>,
    pub end: u64,
    pub checkpoint: Checkpoint,
    pub digest: String,
}

impl Resume {
    pub fn open(path: &Path, header: &Value) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path).context("open resume manifest")?);
        let mut hash = Sha256::new();
        let mut line = Vec::new();
        let mut offset = 0;
        let mut saved = None;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 || !line.ends_with(b"\n") {
                break;
            }
            let value: Value =
                serde_json::from_slice(&line).context("invalid complete resume record")?;
            if offset == 0 {
                ensure!(
                    &value == header,
                    "resume header does not match the root identity, collector, or inventory options"
                );
            } else if value["record"] == "directory_complete" {
                let checkpoint: Checkpoint = serde_json::from_value(value)?;
                ensure!(
                    checkpoint.prefix_sha256 == super::record::hex(&hash.clone().finalize()),
                    "resume checkpoint digest mismatch"
                );
                ensure!(
                    checkpoint.state_sha256 == checkpoint.state_digest()?,
                    "resume checkpoint state digest mismatch"
                );
                ensure!(
                    checkpoint.ancestors.len() <= 128,
                    "resume ancestor depth exceeds 128"
                );
                for (index, frame) in checkpoint.ancestors.iter().enumerate() {
                    if index == 0 {
                        ensure!(
                            bytes(&frame.path)?.is_empty(),
                            "resume ancestor root is invalid"
                        );
                    }
                    if let Some(child) = &frame.last_child {
                        let child = bytes(child)?;
                        ensure!(
                            !child.is_empty()
                                && child != b"."
                                && child != b".."
                                && !child.contains(&b'/')
                                && !child.contains(&0),
                            "invalid resume child name"
                        );
                    }
                }
                let mut prefix = hash.clone();
                prefix.update(&line);
                saved = Some((
                    offset + line.len() as u64,
                    checkpoint,
                    super::record::hex(&prefix.finalize()),
                ));
            }
            hash.update(&line);
            offset += line.len() as u64;
        }
        let (end, checkpoint, digest) =
            saved.context("resume manifest has no complete directory checkpoint")?;
        reader.rewind()?;
        Ok(Self {
            reader,
            end,
            checkpoint,
            digest,
        })
    }
}

pub(super) fn bytes(raw: &RawBytes) -> Result<Vec<u8>> {
    decode_hex(&raw.hex)
}

pub(super) fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    ensure!(
        hex.len().is_multiple_of(2),
        "invalid manifest byte encoding"
    );
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}
