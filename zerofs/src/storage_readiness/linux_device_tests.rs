use super::*;

#[test]
fn serial_parser_accepts_one_trimmed_value_and_rejects_unavailable_evidence() {
    assert_eq!(
        parse_serial("EXAMPLE-NVME-0001\n").unwrap(),
        Serial::try_from("EXAMPLE-NVME-0001".to_owned()).unwrap()
    );
    for invalid in [
        "",
        " \n",
        "EXAMPLE-ONE\nEXAMPLE-TWO",
        "EXAMPLE-ONE\tEXAMPLE-TWO",
    ] {
        assert!(parse_serial(invalid).is_err());
    }
}

fn status(rows: &str) -> String {
    format!(
        "  pool: fixture\n state: ONLINE\nconfig:\n\n        NAME STATE READ WRITE CKSUM\n        fixture ONLINE 0 0 0\n{rows}\nerrors: No known data errors\n"
    )
}

#[test]
fn zpool_parser_collects_all_device_classes_without_using_ordinals_as_identity() {
    let input = status(
        "          mirror-0 ONLINE 0 0 0\n            /dev/disk/by-id/ata-EXAMPLE-ONE-part1 ONLINE 0 0 0\n            /dev/disk/by-id/ata-EXAMPLE-TWO-part1 ONLINE 0 0 0\n        special\n          /dev/disk/by-id/nvme-EXAMPLE-THREE ONLINE 0 0 0\n        cache\n          /dev/disk/by-id/nvme-EXAMPLE-FOUR ONLINE 0 0 0",
    );
    assert_eq!(
        zpool_device_paths(&input, "fixture").unwrap(),
        [
            PathBuf::from("/dev/disk/by-id/ata-EXAMPLE-ONE-part1"),
            PathBuf::from("/dev/disk/by-id/ata-EXAMPLE-TWO-part1"),
            PathBuf::from("/dev/disk/by-id/nvme-EXAMPLE-THREE"),
            PathBuf::from("/dev/disk/by-id/nvme-EXAMPLE-FOUR"),
        ]
    );
}

#[test]
fn missing_offline_or_unrecognized_zfs_backing_fails_closed() {
    for rows in [
        "",
        "          123456 UNAVAIL 0 0 0 was /dev/disk/by-id/ata-EXAMPLE",
        "          /dev/disk/by-id/ata-EXAMPLE OFFLINE 0 0 0",
        "          unknown-vdev ONLINE 0 0 0",
    ] {
        assert!(
            zpool_device_paths(&status(rows), "fixture").is_err(),
            "{rows}"
        );
    }
    assert!(
        zpool_device_paths(
            &status("          /dev/disk/by-id/ata-EXAMPLE ONLINE 0 0 0"),
            "different-pool"
        )
        .is_err()
    );
}
