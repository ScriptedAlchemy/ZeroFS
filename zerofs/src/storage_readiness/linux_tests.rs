use super::*;
use std::cell::RefCell;

const UUID: &str = "47166772-65c1-4ecf-8066-935452891add";

fn mount(filesystem: &str) -> Mount {
    containing_mount(
        &format!("25 1 8:17 / /fixture/storage rw - {filesystem} tank/data rw\n"),
        Path::new("/fixture/storage/data"),
    )
    .unwrap()
}

struct FakeProbe {
    zfs: String,
    pool: String,
    blkid: String,
    fail: bool,
    calls: RefCell<Vec<String>>,
}

impl Default for FakeProbe {
    fn default() -> Self {
        Self {
            zfs: "guid\t201\nmounted\tyes\nmountpoint\t/fixture/storage\nkeystatus\tavailable\nreadonly\toff\n".into(),
            pool: "101\n".into(), blkid: format!("{UUID}\n"), fail: false, calls: RefCell::new(Vec::new()),
        }
    }
}

impl Probe for FakeProbe {
    fn output(&self, program: &str, args: &[&str]) -> Result<String> {
        self.calls.borrow_mut().push(program.to_owned());
        if self.fail {
            bail!("synthetic probe failure")
        }
        match program {
            "zfs" => {
                assert_eq!(
                    args,
                    [
                        "get",
                        "-H",
                        "-p",
                        "-o",
                        "property,value",
                        "guid,mounted,mountpoint,keystatus,readonly",
                        "tank/data"
                    ]
                );
                Ok(self.zfs.clone())
            }
            "zpool" => {
                assert_eq!(args, ["get", "-H", "-p", "-o", "value", "guid", "tank"]);
                Ok(self.pool.clone())
            }
            "blkid" => {
                assert_eq!(args, ["-p", "-s", "UUID", "-o", "value", "/dev/block/8:17"]);
                Ok(self.blkid.clone())
            }
            _ => panic!("unexpected probe {program}"),
        }
    }
}

#[test]
fn zfs_observations_require_pool_and_dataset_guids_mount_and_key_state() {
    let probe = FakeProbe::default();
    let facts = identify(mount("zfs"), &probe).unwrap();
    assert_eq!(
        facts.identity,
        FilesystemIdentity::Zfs {
            pool_guid: "101".into(),
            dataset_guid: "201".into()
        }
    );
    assert!(facts.unlocked && facts.writable);
    assert_eq!(*probe.calls.borrow(), ["zfs", "zpool"]);
}

#[test]
fn locked_and_readonly_zfs_are_facts_never_ready_defaults() {
    for (from, to, writable, unlocked) in [
        ("available", "unavailable", true, false),
        ("available", "none", true, true),
        ("readonly\toff", "readonly\ton", false, true),
    ] {
        let mut probe = FakeProbe::default();
        probe.zfs = probe.zfs.replace(from, to);
        let facts = identify(mount("zfs"), &probe).unwrap();
        assert_eq!((facts.writable, facts.unlocked), (writable, unlocked));
    }
}

#[test]
fn zfs_missing_unknown_duplicate_and_unmounted_evidence_is_rejected() {
    for (from, to) in [
        ("guid\t201\n", ""),
        ("guid\t201", "guid\t0"),
        ("mounted\tyes", "mounted\tno"),
        ("mountpoint\t/fixture/storage", "mountpoint\t/other"),
        ("keystatus\tavailable", "keystatus\tunknown"),
        ("keystatus\tavailable\n", ""),
        ("readonly\toff", "readonly\tunknown"),
        ("readonly\toff", "readonly\toff\nreadonly\toff"),
    ] {
        let mut probe = FakeProbe::default();
        probe.zfs = probe.zfs.replace(from, to);
        assert!(
            identify(mount("zfs"), &probe).is_err(),
            "accepted {from:?} -> {to:?}"
        );
    }
    for pool in ["", "0", "unknown", "101\n102\n"] {
        let probe = FakeProbe {
            pool: pool.into(),
            ..Default::default()
        };
        assert!(identify(mount("zfs"), &probe).is_err());
    }
}

#[test]
fn ext4_and_xfs_use_uncached_uuid_of_actual_mount_device() {
    for filesystem in ["ext4", "xfs"] {
        let probe = FakeProbe::default();
        let facts = identify(mount(filesystem), &probe).unwrap();
        assert_eq!(
            facts.identity,
            FilesystemIdentity::Uuid { uuid: UUID.into() }
        );
        assert_eq!(*probe.calls.borrow(), ["blkid"]);
        for invalid in [
            "",
            "guessed",
            "00000000-0000-0000-0000-000000000000",
            &format!("{UUID}\n{UUID}"),
        ] {
            let probe = FakeProbe {
                blkid: invalid.into(),
                ..Default::default()
            };
            assert!(identify(mount(filesystem), &probe).is_err());
        }
    }
}

#[test]
fn unsupported_filesystems_and_failed_probes_do_not_fall_back() {
    for filesystem in ["apfs", "btrfs", "overlay", "tmpfs", "nfs"] {
        let probe = FakeProbe::default();
        assert!(identify(mount(filesystem), &probe).is_err());
        assert!(probe.calls.borrow().is_empty());
    }
    for filesystem in ["zfs", "ext4", "xfs"] {
        let probe = FakeProbe {
            fail: true,
            ..Default::default()
        };
        assert!(identify(mount(filesystem), &probe).is_err());
    }
}

#[test]
fn nearest_mount_and_mountinfo_escapes_are_resolved_without_prefix_confusion() {
    let input = "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n\
        2 1 8:17 / /fixture/storage rw,nosuid shared:2 - xfs /dev/sdb1 rw\n\
        3 2 0:35 / /fixture/storage/nested\\040dataset rw - zfs tank/data rw\n";
    let mount = containing_mount(input, Path::new("/fixture/storage/nested dataset/data")).unwrap();
    assert_eq!(
        mount.mountpoint,
        Path::new("/fixture/storage/nested dataset")
    );
    assert_eq!(mount.device, (0, 35));
    let other = containing_mount(input, Path::new("/fixture/storage-other/data")).unwrap();
    assert_eq!(other.mountpoint, Path::new("/"));
    assert_eq!(
        unescape("tab\\011and\\134slash\\012").unwrap(),
        "tab\tand\\slash\n"
    );
}

#[test]
fn malformed_and_ambiguous_mount_tables_fail_closed() {
    for input in [
        "",
        "invalid",
        "1 0 8:1 / / rw - ext4",
        "1 0 8:1 / relative rw - ext4 /dev/sda1 rw",
        "1 0 bad / / rw - ext4 /dev/sda1 rw",
        "1 0 8:1 / /bad\\999 rw - ext4 /dev/sda1 rw",
        "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n2 0 8:2 / / rw - xfs /dev/sdb1 rw",
    ] {
        assert!(
            containing_mount(input, Path::new("/fixture/storage/data")).is_err(),
            "accepted {input:?}"
        );
    }
}

#[test]
fn mount_readonly_and_subdirectory_roots_are_preserved() {
    for (options, super_options) in [("ro", "rw"), ("rw", "ro"), ("rw,ro", "rw")] {
        let input = format!(
            "25 1 8:17 /subdirectory /fixture/storage {options} - ext4 /dev/sdb1 {super_options}"
        );
        let facts = identify(
            containing_mount(&input, Path::new("/fixture/storage/data")).unwrap(),
            &FakeProbe::default(),
        )
        .unwrap();
        assert!(!facts.writable);
        assert_eq!(facts.filesystem_root, Path::new("/subdirectory"));
    }
}

#[test]
fn descendant_mounts_below_every_storage_role_are_rejected() {
    for role in ["authority", "catalog", "member", "cache", "journal"] {
        let root = format!("/fixture/{role}");
        for descendant in ["bucket_12345678", "nested/other", "pool.redb"] {
            let input = format!(
                "1 0 8:1 / /fixture rw - ext4 /dev/sda1 rw\n2 1 0:35 / {root}/{descendant} rw - tmpfs tmpfs rw\n"
            );
            let error = containing_mount(&input, Path::new(&root)).unwrap_err();
            assert!(format!("{error:#}").contains("descendant mount"));
        }
    }
}
