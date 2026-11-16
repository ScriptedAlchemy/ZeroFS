use super::*;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use tempfile::TempDir;

// Parallel fixture tests panic the macOS 27.2 kernel on machines with many cores.
static FIXTURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn fixture() -> TempDir {
    let temp = tempfile::Builder::new()
        .prefix("zerofs-inventory-test-")
        .tempdir()
        .unwrap();
    eprintln!("INVENTORY_FIXTURE {}", temp.path().display());
    temp
}

fn scan(root: &Path, hash: bool) -> (bool, Vec<u8>, Vec<Value>) {
    let mut bytes = Vec::new();
    let complete = walk::inventory(root, hash, None, &mut bytes).unwrap();
    let records = bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    (complete, bytes, records)
}

fn entry<'a>(records: &'a [Value], path: &str) -> &'a Value {
    records
        .iter()
        .find(|r| r["record"] == "entry" && r["path"]["display"] == path)
        .unwrap()
}

fn summary(records: &[Value]) -> &Value {
    records.iter().find(|r| r["record"] == "summary").unwrap()
}

#[test]
fn unsupported_metadata_formats_are_complete_but_extent_failures_are_not() {
    let mut output = Manifest::new(Vec::new());
    output.count_unavailable(&serde_json::json!({
        "status": "unavailable", "reason": "format_not_supported_by_filesystem", "errno": 95
    }));
    assert!(output.complete);
    assert_eq!(output.reasons["format_not_supported_by_filesystem"], 1);
    output.count_unavailable(&serde_json::json!({
        "status": "unavailable", "reason": "not_supported_by_filesystem", "errno": 22
    }));
    assert!(!output.complete);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_plain_tree_is_complete() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    fs::write(temp.path().join("file"), b"data").unwrap();
    fs::create_dir(temp.path().join("directory")).unwrap();
    symlink("file", temp.path().join("link")).unwrap();
    let (complete, _, records) = scan(temp.path(), false);
    let visible = platform::trusted_xattr_namespace().visible == Some(true);
    assert_eq!(complete, visible, "{records:#?}");
    assert_eq!(summary(&records)["complete"], visible);
    for path in ["", "file", "directory"] {
        assert_eq!(
            entry(&records, path)["platform_flags"]["status"],
            "observed"
        );
    }
}

#[test]
fn files_hardlinks_symlinks_and_fifo() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let root = temp.path().join("tree");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("file"), b"inventory fixture").unwrap();
    fs::hard_link(root.join("file"), root.join("link")).unwrap();
    fs::hard_link(root.join("file"), temp.path().join("outside")).unwrap();
    symlink("file", root.join("symlink")).unwrap();
    symlink("missing", root.join("dangling")).unwrap();
    symlink(temp.path(), root.join("outside-dir")).unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(root.join("fifo"))
            .status()
            .unwrap()
            .success()
    );
    let (_, _, records) = scan(&root, true);
    assert_eq!(records[0]["hashing"], "sha256");
    assert_eq!(
        entry(&records, "file")["content_sha256"]["value"],
        hex(&Sha256::digest(b"inventory fixture"))
    );
    let group = records
        .iter()
        .find(|r| r["record"] == "hardlink_group")
        .unwrap();
    assert_eq!(group["nlink"]["value"], 3);
    assert_eq!(group["paths_in_tree"].as_array().unwrap().len(), 2);
    assert_eq!(group["links_outside_tree"]["value"], 1);
    assert_eq!(
        entry(&records, "symlink")["symlink_target"]["value"]["hex"],
        "66696c65"
    );
    assert_eq!(
        entry(&records, "dangling")["symlink_target"]["value"]["display"],
        "missing"
    );
    assert_eq!(entry(&records, "fifo")["kind"]["value"], "fifo");
    #[cfg(target_os = "linux")]
    assert_eq!(entry(&records, "fifo")["xattrs"]["status"], "observed");
    #[cfg(target_os = "macos")]
    assert_eq!(
        entry(&records, "fifo")["xattrs"]["reason"],
        "not_implemented_on_platform"
    );
    assert_eq!(summary(&records)["entries"], 7);
}

#[test]
fn sparse_extents_and_xattrs() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    use std::io::{Seek, SeekFrom};
    let temp = fixture();
    let path = temp.path().join("sparse");
    let mut file = fs::File::create(&path).unwrap();
    file.set_len(16 * 1024 * 1024).unwrap();
    file.seek(SeekFrom::Start(8 * 1024 * 1024)).unwrap();
    file.write_all(b"data").unwrap();
    file.sync_all().unwrap();
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        let hole = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: 0,
            fp_length: 8 * 1024 * 1024,
        };
        // SAFETY: The fixture descriptor is writable and hole has the F_PUNCHHOLE layout.
        assert_eq!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &hole) },
            0
        );
    }
    let plain = temp.path().join("plain");
    fs::write(&plain, []).unwrap();
    let name = "user.zerofs-inventory";
    rustix::fs::setxattr(
        &path,
        name,
        b"raw\0value\xff",
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    let (_, _, records) = scan(temp.path(), false);
    let sparse = entry(&records, "sparse");
    let ranges = sparse["data_extents"]["value"]
        .as_array()
        .expect("fixture filesystem must support extent queries");
    assert!(!ranges.is_empty());
    assert!(
        ranges
            .iter()
            .any(|e| e["offset"].as_u64().unwrap() <= 8 * 1024 * 1024
                && e["offset"].as_u64().unwrap() + e["length"].as_u64().unwrap()
                    >= 8 * 1024 * 1024 + 4)
    );
    assert!(
        ranges
            .iter()
            .map(|e| e["length"].as_u64().unwrap())
            .sum::<u64>()
            < 16 * 1024 * 1024
    );
    let attrs = sparse["xattrs"]["value"].as_array().unwrap();
    assert!(
        attrs.iter().any(|a| a["name"]["display"] == name
            && a["value"]["value"]["hex"] == hex(b"raw\0value\xff"))
    );
    let plain_attrs = entry(&records, "plain")["xattrs"]["value"]
        .as_array()
        .unwrap();
    assert!(!plain_attrs.iter().any(|a| a["name"]["display"] == name));
    use rustix::fd::AsFd;
    let fd = rustix::fs::open(
        &plain,
        rustix::fs::OFlags::RDONLY,
        rustix::fs::Mode::empty(),
    )
    .unwrap();
    assert_eq!(
        collect::xattr(fd.as_fd(), std::ffi::OsStr::new(name)),
        record::Field::Observed(None)
    );
    #[cfg(target_os = "linux")]
    assert!(plain_attrs.is_empty());
}

struct RestorePermissions(std::path::PathBuf);
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[test]
fn unreadable_directory_is_incomplete() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let blocked = temp.path().join("blocked");
    fs::create_dir(&blocked).unwrap();
    fs::write(blocked.join("hidden"), b"hidden").unwrap();
    fs::write(temp.path().join("file"), b"data").unwrap();
    fs::hard_link(temp.path().join("file"), blocked.join("link")).unwrap();
    let restore = RestorePermissions(blocked.clone());
    fs::set_permissions(&blocked, fs::Permissions::from_mode(0o0)).unwrap();
    let (complete, _, records) = scan(temp.path(), false);
    drop(restore);
    assert!(
        !complete,
        "run the permission fixture as an unprivileged user"
    );
    assert_eq!(summary(&records)["complete"], false);
    let unreadable = records
        .iter()
        .position(|r| r["record"] == "directory_unreadable" && r["path"]["display"] == "blocked")
        .unwrap();
    let file = records
        .iter()
        .position(|r| r["record"] == "entry" && r["path"]["display"] == "file")
        .unwrap();
    assert!(unreadable < file);
    assert!(
        !records
            .iter()
            .any(|r| r["path"]["display"] == "blocked/hidden")
    );
    let group = records
        .iter()
        .find(|r| r["record"] == "hardlink_group")
        .unwrap();
    assert_eq!(
        group["links_outside_tree"]["reason"],
        "incomplete_traversal"
    );
}

#[test]
fn root_symlink_and_file_are_rejected() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    symlink(temp.path(), temp.path().join("link")).unwrap();
    fs::write(temp.path().join("file"), b"data").unwrap();
    for path in [
        temp.path().join("link"),
        temp.path().join("link/"),
        temp.path().join("file"),
    ] {
        let mut output = Vec::new();
        assert!(walk::inventory(&path, false, None, &mut output).is_err());
        assert!(output.is_empty());
    }
}

#[test]
fn deterministic_jsonl_and_digest() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    for name in ["z", "b", "a"] {
        fs::write(temp.path().join(name), name).unwrap();
    }
    scan(temp.path(), false);
    let (complete, first, records) = scan(temp.path(), false);
    let (_, second, _) = scan(temp.path(), false);
    assert_eq!(first, second);
    assert_eq!(records[0]["record"], "header");
    assert_eq!(records[0]["hashing"], "not_requested");
    assert_eq!(summary(&records)["complete"], complete);
    let lines = first.split_inclusive(|b| *b == b'\n').collect::<Vec<_>>();
    let preceding = lines[..lines.len() - 1].concat();
    assert_eq!(
        records.last().unwrap()["sha256"],
        hex(&Sha256::digest(preceding))
    );
    let paths = records
        .iter()
        .filter(|r| r["record"] == "entry")
        .map(|r| r["path"]["display"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(paths, ["", "a", "b", "z"]);
    #[cfg(target_os = "macos")]
    {
        assert!(complete, "{records:#?}");
        assert_eq!(
            entry(&records, "a")["acl"]["value"][0]["raw"]["value"],
            Value::Null
        );
    }
}

#[test]
fn directory_replacement_keeps_the_open_anchor() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    struct ReplacingWriter<'a> {
        root: &'a Path,
        outside: &'a Path,
        bytes: Vec<u8>,
        replaced: bool,
    }
    impl Write for ReplacingWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.replaced
                && let Ok(record) = serde_json::from_slice::<Value>(bytes)
                && record["record"] == "entry"
                && record["path"]["display"] == "directory"
            {
                fs::rename(self.root.join("directory"), self.root.join("moved"))?;
                symlink(self.outside, self.root.join("directory"))?;
                self.replaced = true;
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let temp = fixture();
    let root = temp.path().join("root");
    let outside = temp.path().join("outside");
    fs::create_dir_all(root.join("directory")).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(root.join("directory/owned"), b"owned").unwrap();
    fs::write(outside.join("secret"), b"outside").unwrap();
    let mut writer = ReplacingWriter {
        root: &root,
        outside: &outside,
        bytes: Vec::new(),
        replaced: false,
    };
    assert!(!walk::inventory(&root, true, None, &mut writer).unwrap());
    assert!(writer.replaced);
    let records = writer
        .bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        entry(&records, "directory/owned")["content_sha256"]["value"],
        hex(&Sha256::digest(b"owned"))
    );
    assert!(!records.iter().any(|r| {
        r["path"]["display"]
            .as_str()
            .is_some_and(|p| p.contains("secret"))
    }));
    assert!(records.iter().any(|r| r["record"] == "entry_changed"));
}

fn snapshot(path: &Path) -> Vec<Value> {
    use std::os::unix::ffi::OsStrExt;
    let mut paths = vec![path.to_path_buf()];
    #[cfg(target_os = "macos")]
    paths.extend(fs::read_dir(path).unwrap().map(|e| e.unwrap().path()));
    #[cfg(target_os = "linux")]
    {
        let fd = rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOATIME,
            rustix::fs::Mode::empty(),
        )
        .unwrap();
        for item in rustix::fs::Dir::new(fd).unwrap() {
            let item = item.unwrap();
            let name = item.file_name().to_bytes();
            if name != b"." && name != b".." {
                paths.push(path.join(std::ffi::OsStr::from_bytes(name)));
            }
        }
    }
    paths.sort();
    paths.into_iter().map(|p| {
        let m = fs::symlink_metadata(&p).unwrap();
        let names = collect::variable_bytes(|buf| rustix::fs::llistxattr(&p, buf)).unwrap();
        let mut attrs = BTreeMap::new();
        for name in names.split(|b| *b == 0).filter(|n| !n.is_empty()) {
            let value = collect::variable_bytes(|buf| rustix::fs::lgetxattr(&p, std::ffi::OsStr::from_bytes(name), buf)).unwrap();
            attrs.insert(hex(name), hex(&value));
        }
        let observed = serde_json::json!({"path": p.as_os_str().as_bytes(), "mode": m.mode(), "uid": m.uid(), "gid": m.gid(),
            "mtime": [m.mtime(), m.mtime_nsec()], "ctime": [m.ctime(), m.ctime_nsec()], "xattrs": attrs});
        // readlinkat can update symlink atime even when file opens use O_NOATIME.
        #[cfg(target_os = "linux")]
        let observed = {
            let mut observed = observed;
            if !m.file_type().is_symlink() {
                observed["atime"] = serde_json::json!([m.atime(), m.atime_nsec()]);
            }
            observed
        };
        observed
    }).collect()
}

#[test]
fn source_metadata_is_unchanged() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    fs::write(temp.path().join("file"), b"content").unwrap();
    fs::create_dir(temp.path().join("directory")).unwrap();
    symlink("file", temp.path().join("link")).unwrap();
    fs::set_permissions(temp.path().join("file"), fs::Permissions::from_mode(0o640)).unwrap();
    rustix::fs::setxattr(
        temp.path().join("file"),
        "user.zerofs-inventory",
        b"value",
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    #[cfg(target_os = "linux")]
    {
        let file = fs::File::open(temp.path().join("file")).unwrap();
        file.set_times(
            fs::FileTimes::new()
                .set_accessed(std::time::UNIX_EPOCH + std::time::Duration::from_secs(123456)),
        )
        .unwrap();
    }
    let before = snapshot(temp.path());
    let (_, _, records) = scan(temp.path(), true);
    #[cfg(target_os = "linux")]
    assert_eq!(
        records
            .iter()
            .find(|r| r["record"] == "header_update")
            .unwrap()["noatime_fallback"],
        false
    );
    #[cfg(target_os = "macos")]
    let _ = records;
    assert_eq!(before, snapshot(temp.path()));
}

#[cfg(target_os = "macos")]
#[test]
fn macos_extended_acl_and_symlink_metadata() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let path = temp.path().join("file");
    fs::write(&path, b"data").unwrap();
    let result = std::process::Command::new("chmod")
        .args(["+a", "everyone allow read"])
        .arg(&path)
        .status()
        .unwrap();
    assert!(result.success());
    symlink("file", temp.path().join("link")).unwrap();
    let (_, _, records) = scan(temp.path(), false);
    let acl = &entry(&records, "file")["acl"]["value"][0];
    assert_eq!(acl["format"], "macos-extended-text");
    assert!(
        acl["raw"]["value"]["display"]
            .as_str()
            .unwrap()
            .contains(":allow:read")
    );
    assert_eq!(acl["decoded"]["reason"], "not_decoded");
    let link = entry(&records, "link");
    assert_eq!(link["xattrs"]["status"], "observed", "{link:#}");
    assert_eq!(
        link["acl"]["value"][0]["raw"]["status"], "observed",
        "{link:#}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_non_utf8_names_and_symlink_metadata() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let temp = fixture();
    fs::write(temp.path().join(OsStr::from_bytes(b"\xff")), b"data").unwrap();
    let link_path = temp.path().join(OsStr::from_bytes(b"link-\xfe"));
    symlink(OsStr::from_bytes(b"\xff"), &link_path).unwrap();
    rustix::fs::setxattr(
        temp.path().join(OsStr::from_bytes(b"\xff")),
        "user.target-only",
        b"must not follow",
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    let attr_name = "trusted.zerofs-inventory";
    let attr_value = b"link value";
    let set = rustix::fs::lsetxattr(
        &link_path,
        attr_name,
        attr_value,
        rustix::fs::XattrFlags::empty(),
    );
    let has_attr = match set {
        Ok(()) => true,
        Err(rustix::io::Errno::PERM | rustix::io::Errno::ACCESS | rustix::io::Errno::OPNOTSUPP) => {
            false
        }
        Err(error) => panic!("set owned symlink xattr: {error}"),
    };
    let (_, _, records) = scan(temp.path(), false);
    assert!(records.iter().any(|r| r["path"]["hex"] == "ff"));
    assert_eq!(summary(&records)["non_utf8_names"], 2);
    let link = records
        .iter()
        .find(|r| r["path"]["hex"] == "6c696e6b2dfe")
        .unwrap();
    assert_eq!(link["symlink_target"]["value"]["hex"], "ff");
    assert_eq!(link["xattrs"]["status"], "observed");
    let attrs = link["xattrs"]["value"].as_array().unwrap();
    if has_attr {
        assert!(
            attrs.iter().any(|a| a["name"]["display"] == attr_name
                && a["value"]["value"]["hex"] == hex(attr_value))
        );
    } else {
        assert!(attrs.is_empty(), "{attrs:#?}");
    }
    let probes = link["acl"]["value"].as_array().unwrap();
    for probe in &probes[..2] {
        assert_eq!(probe["raw"]["reason"], "not_applicable_on_platform");
    }
    assert_eq!(
        probes[2]["raw"],
        serde_json::json!({
            "status": "unavailable", "reason": "format_not_supported_by_filesystem",
            "errno": rustix::io::Errno::OPNOTSUPP.raw_os_error()
        })
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_raw_acl_probes() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    use rustix::fd::AsFd;
    let temp = fixture();
    let path = temp.path().join("file");
    fs::write(&path, b"data").unwrap();
    let uid = fs::metadata(&path).unwrap().uid();
    let mut acl = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (1_u16, 6_u16, u32::MAX),
        (2, 4, uid + 1),
        (4, 4, u32::MAX),
        (16, 4, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        acl.extend_from_slice(&tag.to_le_bytes());
        acl.extend_from_slice(&permissions.to_le_bytes());
        acl.extend_from_slice(&id.to_le_bytes());
    }
    rustix::fs::setxattr(
        &path,
        "system.posix_acl_access",
        &acl,
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    let fd =
        rustix::fs::open(&path, rustix::fs::OFlags::RDONLY, rustix::fs::Mode::empty()).unwrap();
    let observed = serde_json::to_value(platform::acl(fd.as_fd())).unwrap();
    let probes = observed["value"].as_array().unwrap();
    assert_eq!(probes.len(), 3);
    assert_eq!(probes[0]["raw"]["value"]["hex"], hex(&acl));
    assert_eq!(probes[1]["raw"]["status"], "observed");
    assert_eq!(probes[1]["raw"]["value"], Value::Null);
    assert_eq!(probes[2]["format"], "nfs4-xdr-xattr");
    assert_eq!(
        probes[2]["raw"],
        serde_json::to_value(record::Field::<Option<record::RawBytes>>::Unavailable {
            reason: record::Reason::FormatNotSupportedByFilesystem,
            errno: Some(rustix::io::Errno::OPNOTSUPP.raw_os_error()),
        })
        .unwrap()
    );
    assert_eq!(probes[2]["decoded"]["reason"], "not_decoded");
}

#[cfg(target_os = "linux")]
#[test]
fn linux_inode_flags_include_nodump() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let path = temp.path().join("file");
    let file = fs::File::create(&path).unwrap();
    let original = rustix::fs::ioctl_getflags(&file).unwrap();
    let expected = original | rustix::fs::IFlags::NODUMP;
    rustix::fs::ioctl_setflags(&file, expected).unwrap();
    let (_, _, records) = scan(temp.path(), false);
    assert_eq!(
        entry(&records, "file")["platform_flags"],
        serde_json::json!({
            "status": "observed", "value": expected.bits()
        })
    );
    rustix::fs::ioctl_setflags(&file, original).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn review_trusted_namespace_visibility() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let capabilities = u64::from_str_radix(
        status
            .lines()
            .find_map(|line| line.strip_prefix("CapEff:"))
            .unwrap()
            .trim(),
        16,
    )
    .unwrap();
    let initial_namespace =
        fs::read_link("/proc/self/ns/user").unwrap() == Path::new("user:[4026531837]");
    let visible = initial_namespace && capabilities & (1 << 21) != 0;
    let temp = fixture();
    fs::write(temp.path().join("file"), b"data").unwrap();
    let (complete, _, records) = scan(temp.path(), false);
    assert_eq!(records[0]["trusted_xattr_namespace"]["visible"], visible);
    if !visible {
        for path in ["", "file"] {
            assert_eq!(
                entry(&records, path)["xattrs"]["trusted_namespace"]["reason"],
                "trusted_namespace_not_visible"
            );
        }
        assert!(!complete);
        assert_eq!(summary(&records)["complete"], false);
    }
}

#[test]
fn review_special_node_metadata() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(temp.path().join("fifo"))
            .status()
            .unwrap()
            .success()
    );
    let _socket = std::os::unix::net::UnixListener::bind(temp.path().join("socket")).unwrap();
    let (complete, _, records) = scan(temp.path(), false);
    for path in ["fifo", "socket"] {
        let node = entry(&records, path);
        #[cfg(target_os = "linux")]
        {
            assert_eq!(node["xattrs"]["status"], "observed", "{node:#}");
            let probes = node["acl"]["value"].as_array().unwrap();
            assert_eq!(probes.len(), 3);
            assert_eq!(probes[0]["raw"]["status"], "observed");
        }
        #[cfg(target_os = "macos")]
        for field in ["xattrs", "acl"] {
            assert_eq!(node[field]["reason"], "not_implemented_on_platform");
        }
    }
    #[cfg(target_os = "macos")]
    assert!(!complete);
    #[cfg(target_os = "linux")]
    let _ = complete;
}

#[cfg(target_os = "linux")]
#[test]
fn review_same_device_bind_mount_is_not_traversed() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    if std::env::var("ZEROFS_INVENTORY_BIND_MOUNT_TEST").as_deref() != Ok("1") {
        eprintln!(
            "SKIP real bind mount fixture; set ZEROFS_INVENTORY_BIND_MOUNT_TEST=1 with CAP_SYS_ADMIN"
        );
        return;
    }
    struct Unmount(std::path::PathBuf);
    impl Drop for Unmount {
        fn drop(&mut self) {
            assert!(
                std::process::Command::new("umount")
                    .arg(&self.0)
                    .status()
                    .unwrap()
                    .success()
            );
        }
    }
    let temp = fixture();
    let root = temp.path().join("root");
    let outside = temp.path().join("outside");
    fs::create_dir_all(root.join("mounted")).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("secret"), b"must not hash").unwrap();
    fs::write(root.join("mounted-file"), []).unwrap();
    for (name, kind, major, minor) in [("char", "c", "1", "3"), ("block", "b", "7", "250")] {
        assert!(
            std::process::Command::new("mknod")
                .arg(root.join(name))
                .args([kind, major, minor])
                .status()
                .unwrap()
                .success()
        );
    }
    let mut mounts = Vec::new();
    for (source, target) in [
        (outside.clone(), root.join("mounted")),
        (outside.join("secret"), root.join("mounted-file")),
    ] {
        assert!(
            std::process::Command::new("mount")
                .arg("--bind")
                .arg(source)
                .arg(&target)
                .status()
                .unwrap()
                .success()
        );
        mounts.push(Unmount(target));
    }
    assert_eq!(
        fs::metadata(&root).unwrap().dev(),
        fs::metadata(root.join("mounted")).unwrap().dev()
    );
    let (complete, _, records) = scan(&root, true);
    assert!(
        !records
            .iter()
            .any(|r| r["path"]["display"] == "mounted/secret")
    );
    for path in ["mounted", "mounted-file"] {
        assert!(
            records
                .iter()
                .any(|r| r["record"] == "mount_boundary" && r["path"]["display"] == path)
        );
        assert_ne!(
            entry(&records, path)["content_sha256"]["status"],
            "observed"
        );
    }
    assert!(!complete);
    for path in ["char", "block"] {
        let node = entry(&records, path);
        assert_eq!(node["xattrs"]["status"], "observed", "{node:#}");
        assert_eq!(node["acl"]["value"][0]["raw"]["status"], "observed");
        assert_eq!(node["content_sha256"]["reason"], "not_collected_for_kind");
    }
    drop(mounts);
}

#[test]
fn pacing_limits_entry_starts_and_preserves_output() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    for name in ["a", "b", "c"] {
        fs::write(temp.path().join(name), b"data").unwrap();
    }
    scan(temp.path(), false);
    let (_, unlimited, _) = scan(temp.path(), false);
    let mut paced = Vec::new();
    let start = std::time::Instant::now();
    walk::inventory(
        temp.path(),
        false,
        std::num::NonZeroU32::new(20),
        &mut paced,
    )
    .unwrap();
    assert!(start.elapsed() >= std::time::Duration::from_millis(150));
    assert_eq!(paced, unlimited);
}

#[test]
fn collisions_are_emitted_before_directory_entries() {
    let _guard = FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    fs::write(temp.path().join("file"), b"lower").unwrap();
    fs::write(temp.path().join("FILE"), b"upper").unwrap();
    // Case-insensitive fixture filesystems cannot hold both names.
    if fs::read_dir(temp.path()).unwrap().count() != 2 {
        return;
    }
    let (_, _, records) = scan(temp.path(), false);
    let collision = records
        .iter()
        .position(|r| r["record"] == "name_collision")
        .unwrap();
    let first_child = records
        .iter()
        .position(|r| r["record"] == "entry" && r["path"]["display"] == "FILE")
        .unwrap();
    assert!(collision < first_child);
    assert_eq!(records[0]["schema"], 2);
}

#[test]
fn pacing_cli_requires_a_positive_rate() {
    use clap::Parser;
    for rate in ["0", "-1", "no", "4294967296"] {
        assert!(
            crate::cli::Cli::try_parse_from([
                "zerofs",
                "inventory",
                "/tmp",
                "--max-entries-per-second",
                rate,
            ])
            .is_err()
        );
    }
    assert!(
        crate::cli::Cli::try_parse_from([
            "zerofs",
            "inventory",
            "/tmp",
            "--max-entries-per-second",
            "100",
        ])
        .is_ok()
    );
}
