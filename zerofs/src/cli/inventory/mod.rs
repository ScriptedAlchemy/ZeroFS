mod collect;
mod collision;
#[cfg(target_os = "linux")]
#[path = "collect_linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "collect_macos.rs"]
mod platform;
pub(crate) mod record;
mod resume;
mod spill;
mod walk;

use anyhow::Result;
use record::{Reason, Record, Summary, hex};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, clap::ValueEnum)]
pub(crate) enum HashAlgorithm {
    Sha256,
}

pub(crate) fn run(
    root: &Path,
    hash: Option<HashAlgorithm>,
    max_entries_per_second: Option<std::num::NonZeroU32>,
    work_dir: Option<&Path>,
    resume_from: Option<&Path>,
) -> Result<()> {
    let stdout = std::io::stdout();
    let complete = walk::inventory_with_options(
        root,
        hash.is_some(),
        max_entries_per_second,
        walk::Options {
            work_dir,
            resume_from,
            ..Default::default()
        },
        std::io::BufWriter::new(stdout.lock()),
    )?;
    if !complete {
        std::process::exit(3);
    }
    Ok(())
}

struct Manifest<W> {
    writer: W,
    hash: Sha256,
    entries: u64,
    non_utf8_names: u64,
    reasons: BTreeMap<String, u64>,
    complete: bool,
}

impl<W: Write> Manifest<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            hash: Sha256::new(),
            entries: 0,
            non_utf8_names: 0,
            reasons: BTreeMap::new(),
            complete: true,
        }
    }

    fn emit(&mut self, record: &Record) -> Result<()> {
        let value = serde_json::to_value(record)?;
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        self.replay(&value, &bytes)
    }

    fn replay(&mut self, value: &Value, bytes: &[u8]) -> Result<()> {
        if value["record"] == "entry" {
            self.entries += 1;
            let path = resume::decode_hex(
                value["path"]["hex"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("entry has no raw path"))?,
            )?;
            if std::str::from_utf8(path.rsplit(|b| *b == b'/').next().unwrap()).is_err() {
                self.non_utf8_names += 1;
            }
        }
        self.count_unavailable(value)?;
        self.writer.write_all(bytes)?;
        self.hash.update(bytes);
        Ok(())
    }

    fn count_unavailable(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Object(object) => {
                if object.get("status").and_then(Value::as_str) == Some("unavailable")
                    && let Some(reason) = object.get("reason").and_then(Value::as_str)
                {
                    *self.reasons.entry(reason.to_owned()).or_default() += 1;
                    let reason: Reason = serde_json::from_value(Value::String(reason.to_owned()))?;
                    if !reason.counts_as_complete() {
                        self.complete = false;
                    }
                }
                for value in object.values() {
                    self.count_unavailable(value)?;
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.count_unavailable(value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(mut self) -> Result<bool> {
        let complete = self.complete;
        self.emit(&Record::Summary(Summary {
            entries: self.entries,
            non_utf8_names: self.non_utf8_names,
            unavailable_by_reason: self.reasons.clone(),
            complete,
        }))?;
        let digest = Record::Digest {
            algorithm: "sha256",
            sha256: hex(&self.hash.finalize()),
        };
        serde_json::to_writer(&mut self.writer, &digest)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(complete)
    }
}

#[cfg(test)]
mod tests;
