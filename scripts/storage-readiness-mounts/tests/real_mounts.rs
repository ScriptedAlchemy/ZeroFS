use super::*;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn command(program: &str, args: &[&str]) -> String {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{program} {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

struct Mount(PathBuf);

impl Mount {
    fn tmpfs(path: &Path) -> Self {
        command(
            "mount",
            &[
                "-t",
                "tmpfs",
                "-o",
                "size=8m",
                "tmpfs",
                path.to_str().unwrap(),
            ],
        );
        Self(path.to_path_buf())
    }

    fn bind(source: &Path, path: &Path) -> Self {
        command(
            "mount",
            &["--bind", source.to_str().unwrap(), path.to_str().unwrap()],
        );
        Self(path.to_path_buf())
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        let status = Command::new("umount").arg(&self.0).status().unwrap();
        assert!(
            status.success(),
            "cannot unmount owned path {}",
            self.0.display()
        );
        println!("Removed the owned mount at {}.", self.0.display());
    }
}

fn authorized() {
    assert_eq!(
        std::env::var("ZEROFS_READINESS_MOUNT_TEST").as_deref(),
        Ok("1")
    );
}

#[test]
#[ignore = "requires explicit Linux mount authority in an isolated container"]
fn real_tmpfs_and_bind_descendants_are_rejected() {
    authorized();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    std::fs::create_dir(&source).unwrap();
    for role in ["authority", "catalog", "member", "cache", "journal"] {
        let root = temp.path().join(role);
        std::fs::create_dir(&root).unwrap();
        let _root_mount = Mount::tmpfs(&root);
        let child = root.join("bucket_12345678");
        std::fs::create_dir(&child).unwrap();
        for bind in [false, true] {
            let mounted = if bind {
                Mount::bind(&source, &child)
            } else {
                Mount::tmpfs(&child)
            };
            let error = linux::observe(&root).unwrap_err();
            assert!(
                format!("{error:#}").contains("descendant mount"),
                "{error:#}"
            );
            println!(
                "The {role} role rejected its {} descendant mount.",
                if bind { "bind" } else { "tmpfs" }
            );
            drop(mounted);
        }
    }
    println!(
        "The real mount-table guard passed. A tmpfs root has no supported filesystem identity."
    );
}

struct LoopDevice {
    device: String,
    node: Option<PathBuf>,
}

impl Drop for LoopDevice {
    fn drop(&mut self) {
        if let Some(node) = &self.node {
            std::fs::remove_file(node).unwrap();
        }
        command("losetup", &["--detach", &self.device]);
        println!("Detached the owned loop device {}.", self.device);
    }
}

#[test]
#[ignore = "requires a separate privileged container and an owned ext4 loop image"]
fn real_ext4_identity_and_post_check_mount_change() {
    authorized();
    assert_eq!(
        std::env::var("ZEROFS_READINESS_LOOP_TEST").as_deref(),
        Ok("1")
    );
    let temp = tempfile::tempdir().unwrap();
    let image = temp.path().join("owned-ext4.img");
    std::fs::File::create(&image)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    command("mkfs.ext4", &["-q", "-F", image.to_str().unwrap()]);
    let device = command("losetup", &["--find", "--show", image.to_str().unwrap()]);
    let mut owned = LoopDevice { device, node: None };
    let root = temp.path().join("journal");
    std::fs::create_dir(&root).unwrap();
    command("mount", &[&owned.device, root.to_str().unwrap()]);
    let mounted = Mount(root.clone());
    let child = root.join("bucket_12345678");
    std::fs::create_dir(&child).unwrap();
    let dev = std::fs::metadata(&root).unwrap().dev();
    let node = PathBuf::from(format!(
        "/dev/block/{}:{}",
        libc::major(dev),
        libc::minor(dev)
    ));
    if !node.exists() {
        std::fs::create_dir_all("/dev/block").unwrap();
        std::os::unix::fs::symlink(&owned.device, &node).unwrap();
        owned.node = Some(node);
    }
    let expected_uuid = command("blkid", &["-p", "-s", "UUID", "-o", "value", &owned.device]);
    let facts = linux::observe(&root).unwrap();
    assert_eq!(
        facts.identity,
        FilesystemIdentity::Uuid {
            uuid: expected_uuid
        }
    );
    assert!(facts.writable && facts.unlocked);
    println!(
        "The production Linux adapter matched the loop-mounted ext4 UUID through real blkid -p."
    );
    let changed = Mount::tmpfs(&child);
    let opened = child.join("opened-after-check");
    std::fs::write(&opened, b"owned mount race fixture").unwrap();
    assert_ne!(std::fs::metadata(&opened).unwrap().dev(), dev);
    println!(
        "The mount change after preflight was not detected. The later path-based open wrote to the replacement filesystem because preflight held no handle."
    );
    assert!(format!("{:#}", linux::observe(&root).unwrap_err()).contains("descendant mount"));
    drop(changed);
    drop(mounted);
    drop(owned);
    drop(temp);
    assert!(!image.exists());
    println!("Removed the owned ext4 image and fixture tree.");
}
