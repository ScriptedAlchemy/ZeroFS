use super::config::DiskProfile;
use super::*;
use serde_json::{Value, json};
use std::cell::Cell;
use std::collections::BTreeMap;

const FAST_UUID: &str = "47166772-65c1-4ecf-8066-935452891add";

struct Fixture {
    _temp: tempfile::TempDir,
    document: Value,
    facts: BTreeMap<PathBuf, MountedFilesystem>,
    calls: Cell<usize>,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().canonicalize().unwrap();
        let durable = base.join("durable");
        let fast = base.join("fast");
        let paths = [
            durable.join("authority"),
            durable.join("catalog"),
            durable.join("data"),
            fast.join("cache"),
            fast.join("journal"),
        ];
        for path in &paths {
            std::fs::create_dir_all(path).unwrap();
        }
        let document = json!({
            "cache": { "dir": paths[3], "disk_size_gb": 0.001 },
            "storage": {
                "url": url::Url::from_directory_path(&paths[0]).unwrap().as_str(),
                "encryption_password": "synthetic-readiness-password",
                "pool": {
                    "catalog_dir": paths[1],
                    "members": [{ "id": "data", "url": url::Url::from_directory_path(&paths[2]).unwrap().as_str(),
                        "capacity_gb": 0.01, "reserve_free_gb": 0.001 }]
                },
                "readiness": {
                    "strict": true, "authority": "durable", "catalog": "durable", "cache": "fast", "journal": "fast",
                    "members": { "data": "durable" },
                    "reserves": [{"filesystem": "durable", "bytes": 1_000_000}, {"filesystem": "fast", "bytes": 1_000_000}],
                    "filesystems": [
                        { "id": "durable", "profile": "hdd-cmr", "mountpoint": durable,
                            "identity": { "kind": "zfs", "pool_guid": "101", "dataset_guid": "201" } },
                        { "id": "fast", "profile": "nvme", "mountpoint": fast,
                            "identity": { "kind": "uuid", "uuid": FAST_UUID } }
                    ]
                }
            },
            "writeback": { "enabled": true, "dir": paths[4], "memory_size_gb": 0.001,
                "disk_size_gb": 0.002, "min_free_gb": 0.001 },
            "servers": {}, "telemetry": { "enabled": false }
        });
        let mut facts = BTreeMap::new();
        for (index, path) in paths.into_iter().enumerate() {
            let is_fast = index >= 3;
            facts.insert(
                path,
                MountedFilesystem {
                    mountpoint: if is_fast {
                        fast.clone()
                    } else {
                        durable.clone()
                    },
                    filesystem_root: "/".into(),
                    identity: if is_fast {
                        FilesystemIdentity::Uuid {
                            uuid: FAST_UUID.into(),
                        }
                    } else {
                        FilesystemIdentity::Zfs {
                            pool_guid: "101".into(),
                            dataset_guid: "201".into(),
                        }
                    },
                    writable: true,
                    unlocked: true,
                },
            );
        }
        Self {
            _temp: temp,
            document,
            facts,
            calls: Cell::new(0),
        }
    }

    fn parse(&self) -> Result<Settings> {
        let settings: Settings = toml::from_str(&toml::to_string(&self.document)?)?;
        settings.validate()?;
        Ok(settings)
    }

    fn path(&self, role: &str) -> PathBuf {
        let settings = self.parse().unwrap();
        let roots = settings
            .storage
            .readiness
            .as_ref()
            .unwrap()
            .validated_roots(&settings)
            .unwrap();
        roots
            .iter()
            .find(|root| root.role.to_string() == role)
            .unwrap()
            .path
            .clone()
    }

    fn verify(&self) -> Result<()> {
        verify_with(&self.parse()?, self)
    }
}

impl IdentityProvider for Fixture {
    fn observe(&self, root: &Path) -> Result<MountedFilesystem> {
        self.calls.set(self.calls.get() + 1);
        self.facts
            .get(root)
            .cloned()
            .context("synthetic identity unavailable")
    }
}

fn error_contains(result: Result<()>, expected: &str) {
    let error = format!("{:#}", result.unwrap_err());
    assert!(
        error.contains(expected),
        "expected {expected:?}, got {error}"
    );
}

#[test]
fn declared_profiles_roundtrip_and_never_accept_detection_or_unknown_values() {
    for (name, expected) in [
        ("nvme", DiskProfile::Nvme),
        ("hdd-cmr", DiskProfile::HddCmr),
        ("hdd-smr", DiskProfile::HddSmr),
    ] {
        let profile: DiskProfile = serde_json::from_value(json!(name)).unwrap();
        assert_eq!(profile, expected);
        assert_eq!(serde_json::to_value(profile).unwrap(), json!(name));
    }
    for name in ["auto", "ssd", "hdd", "HDD-CMR", ""] {
        assert!(serde_json::from_value::<DiskProfile>(json!(name)).is_err());
    }
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["filesystems"][0]
        .as_object_mut()
        .unwrap()
        .remove("profile");
    assert!(fixture.parse().is_err());
}

#[test]
fn verifies_every_active_role_with_injected_facts_and_shared_control_filesystem() {
    let fixture = Fixture::new();
    fixture.verify().unwrap();
    assert_eq!(fixture.calls.get(), 5);
    let mut smr = Fixture::new();
    smr.document["storage"]["readiness"]["filesystems"][0]["profile"] = json!("hdd-smr");
    smr.verify().unwrap();
}

#[test]
fn ordinary_deployments_and_explicitly_disabled_checks_never_probe_identity() {
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["strict"] = json!(false);
    fixture.verify().unwrap();
    fixture.document["storage"]
        .as_object_mut()
        .unwrap()
        .remove("readiness");
    fixture.verify().unwrap();
    assert_eq!(fixture.calls.get(), 0);
}

#[test]
fn declarations_survive_file_load_and_settings_serialization() {
    let fixture = Fixture::new();
    let path = fixture._temp.path().join("config.toml");
    std::fs::write(&path, toml::to_string(&fixture.document).unwrap()).unwrap();
    let settings = Settings::from_file(&path).unwrap();
    let serialized = toml::to_string(&settings).unwrap();
    let reloaded: Settings = toml::from_str(&serialized).unwrap();
    reloaded.validate().unwrap();
    verify_with(&reloaded, &fixture).unwrap();
    assert_eq!(fixture.calls.get(), 5);
}

#[test]
fn documented_config_preserves_full_unsigned_zfs_guids() {
    let source = include_str!("../../../docs/examples/storage-readiness.toml");
    let settings: Settings = toml::from_str(source).unwrap();
    settings.validate().unwrap();
    let serialized = toml::to_string(&settings).unwrap();
    let roundtrip: Settings = toml::from_str(&serialized).unwrap();
    roundtrip.validate().unwrap();
    let declaration = &roundtrip.storage.readiness.as_ref().unwrap().filesystems[0];
    assert_eq!(
        declaration.identity,
        FilesystemIdentity::Zfs {
            pool_guid: (u64::MAX - 1).to_string(),
            dataset_guid: u64::MAX.to_string(),
        }
    );
    for invalid in ["0", "01", "+1", "18446744073709551616", "guessed"] {
        let text = source.replace("18446744073709551614", invalid);
        let parsed: Settings = toml::from_str(&text).unwrap();
        assert!(parsed.validate().is_err(), "accepted GUID {invalid}");
    }
}

#[test]
fn missing_wrong_locked_readonly_or_unknown_identity_fails_for_each_role() {
    for role in ["authority", "catalog", "member data", "journal", "cache"] {
        for failure in [
            "missing",
            "wrong-pool",
            "wrong-dataset",
            "unmounted",
            "locked",
            "readonly",
            "unknown",
        ] {
            let mut fixture = Fixture::new();
            let path = fixture.path(role);
            match failure {
                "missing" => std::fs::remove_dir(&path).unwrap(),
                "unknown" => {
                    fixture.facts.remove(&path);
                }
                _ => {
                    let fact = fixture.facts.get_mut(&path).unwrap();
                    match failure {
                        "wrong-pool" => {
                            fact.identity = FilesystemIdentity::Zfs {
                                pool_guid: "999".into(),
                                dataset_guid: "201".into(),
                            }
                        }
                        "wrong-dataset" => {
                            fact.identity = FilesystemIdentity::Zfs {
                                pool_guid: "101".into(),
                                dataset_guid: "999".into(),
                            }
                        }
                        "unmounted" => fact.mountpoint = "/".into(),
                        "locked" => fact.unlocked = false,
                        "readonly" => fact.writable = false,
                        _ => unreachable!(),
                    }
                }
            }
            let error = format!("{:#}", fixture.verify().unwrap_err());
            assert!(error.contains(role), "{failure} / {role}: {error}");
            if failure == "missing" {
                assert!(!path.exists());
            }
        }
    }
}

#[test]
fn empty_fallback_directory_is_not_a_mounted_filesystem() {
    let mut fixture = Fixture::new();
    for facts in fixture.facts.values_mut() {
        facts.mountpoint = "/".into();
    }
    error_contains(fixture.verify(), "declared mounted filesystem root");
}

#[test]
fn changed_uuid_binding_is_rejected() {
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["filesystems"][1]["identity"]["uuid"] =
        json!("202a7f25-f539-4b30-8bb1-0b36331fde71");
    error_contains(fixture.verify(), "identity changed");
}

#[test]
fn bind_subdirectories_and_nested_mounts_are_not_the_declared_root() {
    let mut fixture = Fixture::new();
    let path = fixture.path("member data");
    fixture.facts.get_mut(&path).unwrap().filesystem_root = "/some/subdir".into();
    error_contains(fixture.verify(), "declared mounted filesystem root");
    fixture.facts.get_mut(&path).unwrap().filesystem_root = "/".into();
    fixture.facts.get_mut(&path).unwrap().mountpoint = path.clone();
    error_contains(fixture.verify(), "declared mounted filesystem root");
}

#[test]
fn missing_extra_and_disabled_journal_role_bindings_are_rejected() {
    for key in ["authority", "catalog", "cache", "journal"] {
        let mut fixture = Fixture::new();
        fixture.document["storage"]["readiness"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(fixture.parse().is_err(), "missing {key}");
    }
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["members"] = json!({"other": "durable"});
    error_contains(fixture.parse().map(|_| ()), "missing readiness binding");
    fixture.document["storage"]["readiness"]["members"] =
        json!({"data": "durable", "other": "durable"});
    error_contains(fixture.parse().map(|_| ()), "exactly every");
    let mut fixture = Fixture::new();
    fixture.document["writeback"]["enabled"] = json!(false);
    error_contains(fixture.parse().map(|_| ()), "requires enabled writeback");
    fixture.document["storage"]["readiness"]
        .as_object_mut()
        .unwrap()
        .remove("journal");
    fixture.verify().unwrap();
    assert_eq!(fixture.calls.get(), 4);
}

#[test]
fn remote_roles_separate_wal_and_nvme_durable_roles_are_rejected() {
    let mut fixture = Fixture::new();
    fixture.document["storage"]["pool"]["members"][0]["url"] = json!("s3://synthetic/member");
    error_contains(fixture.parse().map(|_| ()), "local file://");
    let mut fixture = Fixture::new();
    fixture.document["wal"] = json!({"url": "file:///synthetic/wal"});
    error_contains(fixture.parse().map(|_| ()), "separate WAL");
    for index in [0, 1] {
        let mut fixture = Fixture::new();
        fixture.document["storage"]["readiness"]["filesystems"][index]["profile"] =
            json!(if index == 0 { "nvme" } else { "hdd-smr" });
        assert!(fixture.parse().is_err());
    }
}

#[test]
fn duplicate_member_capacity_is_rejected_even_across_zfs_datasets() {
    for same_dataset in [true, false] {
        let mut fixture = Fixture::new();
        let data = fixture.path("member data");
        let mut member = fixture.document["storage"]["pool"]["members"][0].clone();
        member["id"] = json!("other");
        member["url"] = json!(
            url::Url::from_directory_path(data.with_file_name("other-data"))
                .unwrap()
                .as_str()
        );
        fixture.document["storage"]["pool"]["members"]
            .as_array_mut()
            .unwrap()
            .push(member);
        fixture.document["storage"]["readiness"]["members"]["other"] =
            json!(if same_dataset { "durable" } else { "other" });
        if !same_dataset {
            let mut declaration =
                fixture.document["storage"]["readiness"]["filesystems"][0].clone();
            declaration["id"] = json!("other");
            declaration["mountpoint"] = json!(data.parent().unwrap().join("nested-dataset"));
            declaration["identity"]["dataset_guid"] = json!("202");
            fixture.document["storage"]["readiness"]["filesystems"]
                .as_array_mut()
                .unwrap()
                .push(declaration);
            fixture.document["storage"]["pool"]["members"][1]["url"] = json!(
                url::Url::from_directory_path(data.parent().unwrap().join("nested-dataset/data"))
                    .unwrap()
                    .as_str()
            );
        }
        error_contains(fixture.parse().map(|_| ()), "capacity domain");
    }
}

#[test]
fn independent_hdd_member_is_accepted() {
    let mut fixture = Fixture::new();
    let mountpoint = fixture._temp.path().canonicalize().unwrap().join("second");
    let path = mountpoint.join("data");
    std::fs::create_dir_all(&path).unwrap();
    let mut member = fixture.document["storage"]["pool"]["members"][0].clone();
    member["id"] = json!("other");
    member["url"] = json!(url::Url::from_directory_path(&path).unwrap().as_str());
    fixture.document["storage"]["pool"]["members"]
        .as_array_mut()
        .unwrap()
        .push(member);
    fixture.document["storage"]["readiness"]["members"]["other"] = json!("other");
    fixture.document["storage"]["readiness"]["filesystems"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "id": "other", "profile": "hdd-smr", "mountpoint": mountpoint,
            "identity": { "kind": "zfs", "pool_guid": "102", "dataset_guid": "202" }
        }));
    fixture.document["storage"]["readiness"]["reserves"]
        .as_array_mut()
        .unwrap()
        .push(json!({"filesystem": "other", "bytes": 1_000_000}));
    fixture.facts.insert(
        path,
        MountedFilesystem {
            mountpoint,
            filesystem_root: "/".into(),
            identity: FilesystemIdentity::Zfs {
                pool_guid: "102".into(),
                dataset_guid: "202".into(),
            },
            writable: true,
            unlocked: true,
        },
    );
    fixture.verify().unwrap();
    assert_eq!(fixture.calls.get(), 6);
}

#[test]
fn invalid_and_aliased_declarations_fail_without_host_probes() {
    for (pointer, value) in [
        ("/storage/readiness/authority", json!("unknown")),
        (
            "/storage/readiness/filesystems/0/identity/pool_guid",
            json!(0),
        ),
        (
            "/storage/readiness/filesystems/0/identity/dataset_guid",
            json!(0),
        ),
        (
            "/storage/readiness/filesystems/1/identity/uuid",
            json!("guessed"),
        ),
        (
            "/storage/readiness/filesystems/1/identity/uuid",
            json!("00000000-0000-0000-0000-000000000000"),
        ),
        ("/storage/readiness/filesystems/1/id", json!("durable")),
        (
            "/storage/readiness/filesystems/0/mountpoint",
            json!("relative"),
        ),
        (
            "/storage/readiness/filesystems/0/mountpoint",
            json!("/tmp/../wrong"),
        ),
    ] {
        let mut fixture = Fixture::new();
        *fixture.document.pointer_mut(pointer).unwrap() = value;
        assert!(fixture.parse().is_err(), "accepted {pointer}");
        assert_eq!(fixture.calls.get(), 0);
    }
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["filesystems"][1]["identity"] =
        fixture.document["storage"]["readiness"]["filesystems"][0]["identity"].clone();
    error_contains(fixture.parse().map(|_| ()), "aliases");
}

#[test]
fn different_profiles_cannot_describe_the_same_zfs_capacity() {
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["filesystems"][1]["identity"] = json!({
        "kind": "zfs", "pool_guid": "101", "dataset_guid": "202"
    });
    error_contains(fixture.parse().map(|_| ()), "conflicting profiles");
}

#[test]
fn symlinked_roots_cannot_overlap() {
    use std::os::unix::fs::symlink;
    let mut fixture = Fixture::new();
    let authority = fixture.path("authority");
    let alias = authority.with_file_name("catalog-alias");
    symlink(&authority, &alias).unwrap();
    fixture.document["storage"]["pool"]["catalog_dir"] = json!(alias);
    error_contains(fixture.verify(), "must not overlap");
}

#[test]
fn symlinked_mountpoint_fails_runtime_canonical_check() {
    use std::os::unix::fs::symlink;
    let mut fixture = Fixture::new();
    let original = fixture.path("authority").parent().unwrap().to_path_buf();
    let alias = original.with_file_name("mount-alias");
    symlink(&original, &alias).unwrap();
    fixture.document["storage"]["readiness"]["filesystems"][0]["mountpoint"] = json!(alias);
    fixture.document["storage"]["url"] = json!(
        url::Url::from_directory_path(alias.join("authority"))
            .unwrap()
            .as_str()
    );
    fixture.document["storage"]["pool"]["catalog_dir"] = json!(alias.join("catalog"));
    fixture.document["storage"]["pool"]["members"][0]["url"] = json!(
        url::Url::from_directory_path(alias.join("data"))
            .unwrap()
            .as_str()
    );
    error_contains(fixture.verify(), "mountpoint must be canonical");
}

#[test]
fn placement_remains_unavailable_after_a_successful_readiness_check() {
    let mut fixture = Fixture::new();
    fixture.verify().unwrap();
    fixture.document["placement"] = json!({ "enabled": true });
    assert!(fixture.parse().is_err());
    fixture
        .document
        .as_object_mut()
        .unwrap()
        .remove("placement");
    fixture.document["storage"]["readiness"]["placement_enabled"] = json!(true);
    assert!(fixture.parse().is_err());
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn unsupported_host_stops_normal_open_and_explicit_init_before_any_storage_mutation() {
    let fixture = Fixture::new();
    let settings = fixture.parse().unwrap();
    for initialize in [false, true] {
        let error = crate::storage_backend::open_pool_storage(&settings, initialize)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("no host identity adapter"));
    }
    let error = crate::storage_backend::open_storage(&settings)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("no host identity adapter"));
    for role in ["authority", "catalog", "member data", "journal", "cache"] {
        assert_eq!(
            std::fs::read_dir(fixture.path(role)).unwrap().count(),
            0,
            "mutated {role}"
        );
    }
}

#[test]
fn real_open_paths_are_verified_without_creating_missing_children() {
    let fixture = Fixture::new();
    let settings = fixture.parse().unwrap();
    let paths = ["authority", "catalog", "member data", "cache", "journal"]
        .into_iter()
        .map(|role| (role, fixture.path(role).join("actual-child")))
        .collect::<Vec<_>>();
    verify_open_paths_with(&settings, &fixture, &paths).unwrap();
    for (_, path) in paths {
        assert!(!path.exists());
    }
}

#[test]
fn a_real_open_path_cannot_escape_its_declared_role_or_filesystem() {
    use std::os::unix::fs::symlink;
    for role in ["authority", "catalog", "member data", "cache", "journal"] {
        let mut fixture = Fixture::new();
        let root = fixture.path(role);
        let child = root.join("actual-child");
        std::fs::create_dir(&child).unwrap();
        let mut facts = fixture.facts[&root].clone();
        facts.identity = FilesystemIdentity::Uuid {
            uuid: "202a7f25-f539-4b30-8bb1-0b36331fde71".into(),
        };
        fixture.facts.insert(child.clone(), facts);
        error_contains(
            verify_open_paths_with(
                &fixture.parse().unwrap(),
                &fixture,
                &[(role, child.clone())],
            ),
            "identity changed",
        );
        std::fs::remove_dir(&child).unwrap();
        let outside = fixture._temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, &child).unwrap();
        error_contains(
            verify_open_paths_with(&fixture.parse().unwrap(), &fixture, &[(role, child)]),
            "escapes its declared role",
        );
    }
}
#[path = "device_tests.rs"]
mod device_tests;
