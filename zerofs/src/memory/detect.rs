use super::cgroup::{map_cgroup2_mounts, parse_cgroup2_mounts, parse_unified_cgroup};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::collections::BTreeSet;
use std::io;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
pub enum Observation {
    Available(u64),
    Unlimited,
    Unavailable { reason: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitRole {
    Dedicated,
    SharedAncestor,
    HostWide,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    Mapping,
    Hard,
    High,
    HostTotal,
    Swap,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LimitObservation {
    pub source: PathBuf,
    pub role: LimitRole,
    pub kind: LimitKind,
    pub value: Observation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FiniteLimit {
    pub source: PathBuf,
    pub role: LimitRole,
    pub kind: LimitKind,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct DetectedLimits {
    pub observations: Vec<LimitObservation>,
}

impl DetectedLimits {
    pub fn effective_limit(&self) -> Option<FiniteLimit> {
        self.observations
            .iter()
            .filter_map(|row| {
                if let Observation::Available(bytes) = row.value
                    && matches!(
                        row.kind,
                        LimitKind::Hard | LimitKind::High | LimitKind::HostTotal
                    )
                {
                    Some(FiniteLimit {
                        source: row.source.clone(),
                        role: row.role,
                        kind: row.kind,
                        bytes,
                    })
                } else {
                    None
                }
            })
            .min_by_key(|row| row.bytes)
    }
}

pub fn detect_limits() -> Result<DetectedLimits> {
    detect_limits_with(|path| std::fs::read(path))
}

/// Inject Linux file contents in fixtures. Missing and denied files remain observations.
pub fn detect_limits_with(
    mut read: impl FnMut(&Path) -> io::Result<Vec<u8>>,
) -> Result<DetectedLimits> {
    let mut result = DetectedLimits {
        observations: Vec::new(),
    };
    let cgroup = read_mapping(&mut read, &mut result, "/proc/self/cgroup");
    let mounts = read_mapping(&mut read, &mut result, "/proc/self/mountinfo");
    if let (Some(cgroup), Some(mounts)) = (cgroup, mounts) {
        let mounts = parse_cgroup2_mounts(&mounts).context("invalid /proc/self/mountinfo")?;
        if mounts.is_empty()
            || !cgroup
                .split(|byte| *byte == b'\n')
                .any(|line| line.starts_with(b"0::"))
        {
            result.push(
                PathBuf::from("/proc/self/cgroup"),
                LimitRole::Dedicated,
                LimitKind::Mapping,
                Observation::Unavailable {
                    reason: "no visible cgroup v2 mapping".into(),
                },
            );
        } else {
            let current = parse_unified_cgroup(&cgroup).context("invalid /proc/self/cgroup")?;
            validate_path(&current)?;
            for (root, mount) in &mounts {
                validate_path(root)?;
                validate_path(mount)?;
            }
            let mappings = map_cgroup2_mounts(&current, &mounts);
            if mappings.is_empty() {
                bail!("no visible cgroup v2 mount");
            }
            let mut seen = BTreeSet::new();
            for (mount, mut path) in mappings {
                let mut role = LimitRole::Dedicated;
                loop {
                    for (name, kind) in [
                        ("memory.max", LimitKind::Hard),
                        ("memory.high", LimitKind::High),
                    ] {
                        let source = path.join(name);
                        if !seen.insert(source.clone()) {
                            continue;
                        }
                        let value = match read(&source) {
                            Ok(raw) => {
                                let raw = std::str::from_utf8(&raw)
                                    .with_context(|| {
                                        format!("invalid limit at {}", source.display())
                                    })?
                                    .trim();
                                if raw == "max" {
                                    Observation::Unlimited
                                } else {
                                    if raw.is_empty()
                                        || !raw.bytes().all(|byte| byte.is_ascii_digit())
                                    {
                                        bail!(
                                            "invalid cgroup limit {raw:?} at {}",
                                            source.display()
                                        );
                                    }
                                    Observation::Available(raw.parse().with_context(|| {
                                        format!("invalid cgroup limit at {}", source.display())
                                    })?)
                                }
                            }
                            Err(error) => unavailable(error),
                        };
                        result.push(source, role, kind, value);
                    }
                    if path == mount {
                        break;
                    }
                    path = path
                        .parent()
                        .filter(|parent| parent.starts_with(&mount))
                        .context("cgroup path escaped its mount")?
                        .to_path_buf();
                    role = LimitRole::SharedAncestor;
                }
            }
        }
    }
    let source = PathBuf::from("/proc/meminfo");
    match read(&source) {
        Ok(raw) => {
            let raw = std::str::from_utf8(&raw).context("invalid /proc/meminfo")?;
            for (name, kind) in [
                ("MemTotal", LimitKind::HostTotal),
                ("SwapTotal", LimitKind::Swap),
            ] {
                let mut values = raw
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .filter(|(key, _)| *key == name);
                let value = if let Some((_, raw)) = values.next() {
                    if values.next().is_some() {
                        bail!("duplicate {name} in /proc/meminfo");
                    }
                    let fields: Vec<_> = raw.split_whitespace().collect();
                    if fields.len() != 2
                        || fields[1] != "kB"
                        || !fields[0].bytes().all(|byte| byte.is_ascii_digit())
                    {
                        bail!("invalid {name} in /proc/meminfo");
                    }
                    let kb: u64 = fields[0]
                        .parse()
                        .with_context(|| format!("invalid {name} in /proc/meminfo"))?;
                    Observation::Available(
                        kb.checked_mul(1024)
                            .with_context(|| format!("{name} overflow in /proc/meminfo"))?,
                    )
                } else {
                    Observation::Unavailable {
                        reason: format!("{name} is absent from /proc/meminfo"),
                    }
                };
                result.push(source.clone(), LimitRole::HostWide, kind, value);
            }
        }
        Err(error) => {
            let value = unavailable(error);
            result.push(
                source.clone(),
                LimitRole::HostWide,
                LimitKind::HostTotal,
                value.clone(),
            );
            result.push(source, LimitRole::HostWide, LimitKind::Swap, value);
        }
    }
    Ok(result)
}

impl DetectedLimits {
    fn push(&mut self, source: PathBuf, role: LimitRole, kind: LimitKind, value: Observation) {
        self.observations.push(LimitObservation {
            source,
            role,
            kind,
            value,
        });
    }
}

fn unavailable(error: io::Error) -> Observation {
    Observation::Unavailable {
        reason: format!("{:?}: {error}", error.kind()),
    }
}

fn read_mapping(
    read: &mut impl FnMut(&Path) -> io::Result<Vec<u8>>,
    result: &mut DetectedLimits,
    name: &str,
) -> Option<Vec<u8>> {
    match read(Path::new(name)) {
        Ok(raw) => Some(raw),
        Err(error) => {
            result.push(
                PathBuf::from(name),
                LimitRole::Dedicated,
                LimitKind::Mapping,
                unavailable(error),
            );
            None
        }
    }
}

fn validate_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        bail!("invalid cgroup path {}", path.display());
    }
    Ok(())
}
