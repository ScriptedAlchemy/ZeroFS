use super::*;

fn hardware(fixture: &mut Fixture) {
    fixture.document["storage"]["readiness"]["reserves"] = json!([
        {"filesystem": "durable", "bytes": 1_000_000},
        {"filesystem": "fast", "bytes": 1_000_000}
    ]);
    fixture.document["storage"]["readiness"]["devices"] = json!([
        {"serial": "EXAMPLE-HDD-0001", "by_id": "/dev/disk/by-id/ata-EXAMPLE-HDD-0001",
         "profile": "hdd-cmr", "roles": ["authority", "catalog", "member data"], "boot": false},
        {"serial": "EXAMPLE-NVME-0001", "by_id": "/dev/disk/by-id/nvme-EXAMPLE-NVME-0001",
         "profile": "nvme", "roles": ["cache", "journal"], "boot": false,
         "rated_host_writes": {"bytes": 220_000_000_000_000_u64, "source": "Synthetic vendor lifetime host-write rating"}},
        {"serial": "EXAMPLE-BOOT-0001", "by_id": "/dev/disk/by-id/ata-EXAMPLE-BOOT-0001",
         "profile": "hdd-cmr", "roles": [], "boot": true}
    ]);
}

#[test]
fn full_device_declarations_survive_file_load_serialization_and_debug() {
    let mut fixture = Fixture::new();
    hardware(&mut fixture);
    let path = fixture._temp.path().join("hardware.toml");
    std::fs::write(&path, toml::to_string(&fixture.document).unwrap()).unwrap();
    let settings = Settings::from_file(&path).unwrap();
    let text = toml::to_string(&settings).unwrap();
    let reloaded: Settings = toml::from_str(&text).unwrap();
    reloaded.validate().unwrap();
    let actual = serde_json::to_value(&reloaded).unwrap();
    for key in ["devices", "reserves"] {
        assert_eq!(
            actual["storage"]["readiness"][key],
            fixture.document["storage"]["readiness"][key]
        );
    }
    let debug = format!("{:?}", settings.storage);
    for expected in [
        "EXAMPLE-NVME-0001",
        "220000000000000",
        "Synthetic vendor",
        "reserves",
    ] {
        assert!(debug.contains(expected), "{debug}");
    }
}

#[test]
fn device_and_reserve_rules_apply_in_both_strict_modes() {
    for strict in [true, false] {
        for (pointer, value, expected) in [
            (
                "/storage/readiness/devices/1/rated_host_writes/bytes",
                json!(0),
                "rated_host_writes",
            ),
            (
                "/storage/readiness/devices/1/rated_host_writes/source",
                json!("  "),
                "source",
            ),
            (
                "/storage/readiness/reserves",
                json!([{"filesystem": "durable", "bytes": 1_000_000}]),
                "missing reserve",
            ),
            (
                "/storage/readiness/reserves/0/bytes",
                json!(999_999),
                "reserve_free_gb",
            ),
            (
                "/storage/readiness/reserves/1/bytes",
                json!(999_999),
                "min_free_gb",
            ),
            (
                "/storage/readiness/reserves/1/filesystem",
                json!("durable"),
                "one reserve",
            ),
            ("/storage/readiness/devices/1/boot", json!(true), "boot"),
            (
                "/storage/readiness/devices/1/profile",
                json!("hdd-cmr"),
                "journal",
            ),
            (
                "/storage/readiness/devices/1/serial",
                json!("EXAMPLE-HDD-0001"),
                "duplicate serial",
            ),
            (
                "/storage/readiness/devices/1/by_id",
                json!("/dev/disk/by-id/ata-EXAMPLE-HDD-0001"),
                "duplicate by-id",
            ),
            (
                "/storage/readiness/devices/1/roles",
                json!(["member missing"]),
                "undeclared role",
            ),
            (
                "/storage/readiness/devices/1/roles",
                json!(["unknown"]),
                "role",
            ),
            ("/storage/readiness/devices/1/serial", json!(""), "serial"),
            (
                "/storage/readiness/devices/1/serial",
                json!(" \t"),
                "serial",
            ),
            (
                "/storage/readiness/devices/1/by_id",
                json!("/dev/nvme0n1"),
                "by-id",
            ),
            (
                "/storage/readiness/devices/1/by_id",
                json!("/dev/disk/by-id/nvme-EXAMPLE-part1"),
                "whole device",
            ),
        ] {
            let mut fixture = Fixture::new();
            hardware(&mut fixture);
            fixture.document["storage"]["readiness"]["strict"] = json!(strict);
            fixture.parse().unwrap();
            *fixture.document.pointer_mut(pointer).unwrap() = value;
            error_contains(fixture.parse().map(|_| ()), expected);
            assert_eq!(fixture.calls.get(), 0);
        }
    }
}

#[test]
fn activation_keys_are_rejected_at_every_declaration_boundary_in_both_modes() {
    for strict in [true, false] {
        for key in ["activate", "enabled", "active"] {
            for pointer in [
                "/storage/readiness",
                "/storage/readiness/devices/1",
                "/storage/readiness/devices/1/rated_host_writes",
                "/storage/readiness/reserves/0",
            ] {
                let mut fixture = Fixture::new();
                hardware(&mut fixture);
                fixture.document["storage"]["readiness"]["strict"] = json!(strict);
                fixture.parse().unwrap();
                fixture.document.pointer_mut(pointer).unwrap()[key] = json!(true);
                error_contains(fixture.parse().map(|_| ()), "unknown field");
                for root in fixture.facts.keys() {
                    assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
                }
            }
        }
    }
}

#[test]
fn boot_and_reserve_declarations_are_required_without_defaults() {
    for strict in [true, false] {
        for pointer in ["/storage/readiness/devices/1", "/storage/readiness"] {
            let mut fixture = Fixture::new();
            hardware(&mut fixture);
            fixture.document["storage"]["readiness"]["strict"] = json!(strict);
            fixture.parse().unwrap();
            let key = if pointer.ends_with('1') {
                "boot"
            } else {
                "reserves"
            };
            fixture
                .document
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(key);
            error_contains(fixture.parse().map(|_| ()), key);
        }
    }
}

#[test]
fn rated_write_overflow_text_is_rejected_without_clamping_in_both_modes() {
    for strict in [true, false] {
        let mut fixture = Fixture::new();
        hardware(&mut fixture);
        fixture.document["storage"]["readiness"]["strict"] = json!(strict);
        fixture.parse().unwrap();
        let text = toml::to_string(&fixture.document).unwrap();
        assert!(text.contains("220000000000000"));
        for invalid in ["18446744073709551616", "\"18446744073709551616\""] {
            let source = text.replace("220000000000000", invalid);
            assert!(toml::from_str::<Settings>(&source).is_err());
        }
    }
}

#[test]
fn hdd_cache_is_accepted_while_journal_stays_on_nvme() {
    for strict in [true, false] {
        let mut fixture = Fixture::new();
        hardware(&mut fixture);
        let durable = fixture.path("authority").parent().unwrap().to_path_buf();
        fixture.document["storage"]["readiness"]["strict"] = json!(strict);
        fixture.document["cache"]["dir"] = json!(durable.join("cache"));
        fixture.document["storage"]["readiness"]["cache"] = json!("durable");
        fixture.document["storage"]["readiness"]["devices"][0]["roles"] =
            json!(["authority", "catalog", "member data", "cache"]);
        fixture.document["storage"]["readiness"]["devices"][1]["roles"] = json!(["journal"]);
        fixture.parse().unwrap();
        fixture.document["writeback"]["dir"] = json!(durable.join("journal"));
        fixture.document["storage"]["readiness"]["journal"] = json!("durable");
        error_contains(fixture.parse().map(|_| ()), "journal must declare nvme");
    }
}

#[test]
fn absent_lifetime_rating_is_unavailable_with_a_reason() {
    let mut fixture = Fixture::new();
    hardware(&mut fixture);
    let settings = fixture.parse().unwrap();
    let devices = &settings.storage.readiness.as_ref().unwrap().devices;
    assert_eq!(
        devices[1].rated_host_writes().unwrap().bytes,
        220_000_000_000_000
    );
    assert!(
        devices[0]
            .rated_host_writes()
            .unwrap_err()
            .contains("no lifetime rating was declared")
    );
    let serialized = serde_json::to_value(&devices[0]).unwrap();
    assert!(serialized.get("rated_host_writes").is_none());
}

#[test]
fn reserve_is_declared_once_across_datasets_of_one_pool() {
    for strict in [true, false] {
        let mut fixture = Fixture::new();
        hardware(&mut fixture);
        let mountpoint = fixture._temp.path().canonicalize().unwrap().join("control");
        fixture.document["storage"]["readiness"]["strict"] = json!(strict);
        fixture.document["storage"]["readiness"]["catalog"] = json!("control");
        fixture.document["storage"]["pool"]["catalog_dir"] = json!(mountpoint.join("catalog"));
        fixture.document["storage"]["readiness"]["filesystems"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id": "control", "profile": "hdd-cmr", "mountpoint": mountpoint,
                "identity": {"kind": "zfs", "pool_guid": "101", "dataset_guid": "202"}
            }));
        fixture.parse().unwrap();
        fixture.document["storage"]["readiness"]["reserves"]
            .as_array_mut()
            .unwrap()
            .push(json!({"filesystem": "control", "bytes": 1_000_000}));
        error_contains(fixture.parse().map(|_| ()), "one reserve");
    }
}

#[test]
fn declared_reserve_compares_against_the_existing_pool_default() {
    for strict in [true, false] {
        let mut fixture = Fixture::new();
        hardware(&mut fixture);
        fixture.document["storage"]["readiness"]["strict"] = json!(strict);
        fixture.document["storage"]["pool"]["members"][0]
            .as_object_mut()
            .unwrap()
            .remove("reserve_free_gb");
        error_contains(fixture.parse().map(|_| ()), "reserve_free_gb");
        fixture.document["storage"]["readiness"]["reserves"][0]["bytes"] = json!(1_000_000_000);
        let settings = fixture.parse().unwrap();
        assert_eq!(
            settings.storage.pool.as_ref().unwrap().members[0].reserve_free_gb,
            1.0
        );
        assert_eq!(settings.writeback.as_ref().unwrap().min_free_gb, 0.001);
    }
}
