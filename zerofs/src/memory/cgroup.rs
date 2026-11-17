use anyhow::{Context, Result, bail};
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

pub(crate) fn parse_unified_cgroup(contents: &[u8]) -> Result<PathBuf> {
    for line in contents.split(|byte| *byte == b'\n') {
        let mut fields = line.splitn(3, |byte| *byte == b':');
        if fields.next() == Some(b"0".as_slice()) && fields.next() == Some(b"".as_slice()) {
            let path = fields.next().context("cgroup-v2 entry has no path")?;
            return Ok(PathBuf::from(os_string_from_bytes(path.to_vec())?));
        }
    }
    bail!("/proc/self/cgroup contains no unified cgroup-v2 entry")
}

pub(crate) fn parse_cgroup2_mounts(contents: &[u8]) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut mounts = Vec::new();
    for line in contents.split(|byte| *byte == b'\n') {
        let fields: Vec<_> = line
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .collect();
        let Some(separator) = fields.iter().position(|field| *field == b"-") else {
            continue;
        };
        if fields.get(separator + 1).copied() != Some(b"cgroup2".as_slice()) {
            continue;
        }
        let root = fields
            .get(3)
            .context("cgroup2 mountinfo entry has no root")?;
        let mountpoint = fields
            .get(4)
            .context("cgroup2 mountinfo entry has no mountpoint")?;
        mounts.push((
            PathBuf::from(unescape_mountinfo(root)?),
            PathBuf::from(unescape_mountinfo(mountpoint)?),
        ));
    }
    Ok(mounts)
}

pub(crate) fn map_cgroup2_mounts(
    current: &Path,
    mounts: &[(PathBuf, PathBuf)],
) -> Vec<(PathBuf, PathBuf)> {
    let exact = matching_cgroup2_mounts(current, mounts);
    if !exact.is_empty() {
        return exact;
    }

    // A private cgroup namespace exposes its own root as `/`, while mountinfo
    // may retain the host-side subtree in the mount root field. Infer that
    // namespace root from the shallowest mount, then retain every alias or
    // nested bind that exposes the process cgroup. Keep mountinfo order as the
    // deterministic tie-break when more than one root is equally shallow.
    let Some((root, _)) = mounts
        .iter()
        .min_by_key(|(root, _)| root.components().count())
    else {
        return Vec::new();
    };
    let relative = current.strip_prefix(Path::new("/")).unwrap_or(current);
    matching_cgroup2_mounts(&root.join(relative), mounts)
}

fn matching_cgroup2_mounts(
    current: &Path,
    mounts: &[(PathBuf, PathBuf)],
) -> Vec<(PathBuf, PathBuf)> {
    // A nested bind can expose the current cgroup without exposing a tighter
    // ancestor. Inspect all matching mounts instead of allowing that bind to
    // hide ceilings visible through a broader mount. Retain aliases as well,
    // because a limit can be readable through one mount and not another.
    mounts
        .iter()
        .filter_map(|(root, mountpoint)| {
            current
                .strip_prefix(root)
                .ok()
                .map(|relative| (mountpoint.clone(), mountpoint.join(relative)))
        })
        .collect()
}

pub(crate) fn unescape_mountinfo(value: &[u8]) -> Result<OsString> {
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'\\' {
            let octal = value
                .get(index + 1..index + 4)
                .context("truncated mountinfo escape")?;
            if !octal.iter().all(u8::is_ascii_digit) || octal.iter().any(|byte| *byte > b'7') {
                bail!(
                    "invalid mountinfo escape in {:?}",
                    String::from_utf8_lossy(value)
                );
            }
            let decoded = u16::from(octal[0] - b'0') * 64
                + u16::from(octal[1] - b'0') * 8
                + u16::from(octal[2] - b'0');
            let decoded = u8::try_from(decoded).context("mountinfo escape exceeds one byte")?;
            output.push(decoded);
            index += 4;
        } else {
            output.push(value[index]);
            index += 1;
        }
    }
    os_string_from_bytes(output)
}

fn os_string_from_bytes(value: Vec<u8>) -> Result<OsString> {
    #[cfg(unix)]
    {
        Ok(OsString::from_vec(value))
    }
    #[cfg(not(unix))]
    {
        Ok(String::from_utf8(value)
            .context("Linux cgroup path is not valid UTF-8 on this platform")?
            .into())
    }
}
