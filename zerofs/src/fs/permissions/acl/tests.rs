use super::*;
use crate::fs::types::AuthContext;

const OWNER: u32 = 1000;
const GROUP: u32 = 100;

fn creds(uid: u32, groups: &[u32]) -> Credentials {
    Credentials::from_auth_context(&AuthContext {
        uid,
        gid: groups.first().copied().unwrap_or(65534),
        gid_known: true,
        gids: groups.to_vec(),
        groups_complete: true,
        groups_truncated: false,
    })
}

fn allows(acl: &PosixAcl, c: &Credentials, mode: AccessMode) -> bool {
    acl.permits(OWNER, GROUP, c, mode)
}

#[test]
fn xattr_bytes_round_trip_and_match_libacl_layout() {
    let acl = PosixAcl::from_text("u::rw-,u:2000:r--,g::r--,g:300:rw-,m::rw-,o::---");
    let bytes = acl.to_bytes();
    // Version 2, then tag/perm/id little-endian, as `setfacl` writes.
    assert_eq!(&bytes[..4], &[2, 0, 0, 0]);
    assert_eq!(&bytes[4..12], &[1, 0, 6, 0, 0xff, 0xff, 0xff, 0xff]);
    assert_eq!(&bytes[12..20], &[2, 0, 4, 0, 0xd0, 0x07, 0, 0]);
    assert_eq!(PosixAcl::parse(&bytes).unwrap().unwrap(), acl);
    assert_eq!(
        acl.to_text(),
        "u::rw-,u:2000:r--,g::r--,g:300:rw-,m::rw-,o::---"
    );
}

#[test]
fn removal_encodings_and_invalid_values() {
    assert_eq!(PosixAcl::parse(b""), Ok(None));
    assert_eq!(PosixAcl::parse(&[2, 0, 0, 0]), Ok(None));
    assert_eq!(
        PosixAcl::parse(&[1, 0, 0, 0, 1, 0, 7, 0, 0, 0, 0, 0]),
        Err(FsError::NotSupported)
    );
    assert_eq!(PosixAcl::parse(&[2, 0, 0]), Err(FsError::InvalidArgument));
    let valid = PosixAcl::from_text("u::rwx,g::r-x,o::r-x").to_bytes();
    assert_eq!(
        PosixAcl::parse(&valid[..valid.len() - 1]),
        Err(FsError::InvalidArgument)
    );
    // Named entry without a mask, duplicate named ids, out-of-order tags,
    // missing other, and an unknown permission bit.
    for bad in [
        [1u16, 7, 2, 7, 4, 7, 0x20, 7],
        [1, 7, 4, 7, 0x20, 7, 0x20, 7],
    ] {
        let mut raw = vec![2, 0, 0, 0];
        for pair in bad.chunks(2) {
            raw.extend_from_slice(&pair[0].to_le_bytes());
            raw.extend_from_slice(&pair[1].to_le_bytes());
            raw.extend_from_slice(&5u32.to_le_bytes());
        }
        assert_eq!(PosixAcl::parse(&raw), Err(FsError::InvalidArgument));
    }
    let mut extra_bit = PosixAcl::from_text("u::rwx,g::r-x,o::r-x")
        .to_bytes()
        .to_vec();
    extra_bit[6] = 8;
    assert_eq!(PosixAcl::parse(&extra_bit), Err(FsError::InvalidArgument));
    let mut no_other = PosixAcl::from_text("u::rwx,g::r-x,o::r-x")
        .to_bytes()
        .to_vec();
    no_other.truncate(no_other.len() - 8);
    assert_eq!(PosixAcl::parse(&no_other), Err(FsError::InvalidArgument));
}

#[test]
fn access_decisions_follow_posix_acl_permission() {
    let acl = PosixAcl::from_text("u::rw-,u:2000:rwx,u:2001:---,g::r--,g:300:rw-,m::r-x,o::---");
    // Owner uses the owner entry only.
    assert!(allows(&acl, &creds(OWNER, &[]), AccessMode::Write));
    assert!(!allows(&acl, &creds(OWNER, &[]), AccessMode::Execute));
    // A named user is limited by the mask and ignores group entries.
    let named = creds(2000, &[300]);
    assert!(allows(&acl, &named, AccessMode::Read));
    assert!(allows(&acl, &named, AccessMode::Execute));
    assert!(!allows(&acl, &named, AccessMode::Write));
    // A named user with no permissions stays denied even via other groups.
    assert!(!allows(&acl, &creds(2001, &[GROUP]), AccessMode::Read));
    // The owning group and a named group, both limited by the mask.
    assert!(allows(&acl, &creds(3000, &[GROUP]), AccessMode::Read));
    assert!(!allows(&acl, &creds(3000, &[300]), AccessMode::Write));
    assert!(allows(&acl, &creds(3000, &[300]), AccessMode::Read));
    // Other.
    assert!(!allows(&acl, &creds(3000, &[]), AccessMode::Read));
}

#[test]
fn named_users_and_groups_require_defined_ids() {
    let acl = PosixAcl::from_text("u::rwx,u:2000:r--,g::r--,g:300:r--,m::r--,o::---");
    for tag in [TAG_USER, TAG_GROUP] {
        // The reserved sentinel is invalid, not the upper half of the ID space.
        for id in [UNDEFINED_ID, UNDEFINED_ID - 1] {
            let mut raw = acl.to_bytes().to_vec();
            let entry = raw[HEADER_LEN..]
                .chunks_exact_mut(ENTRY_LEN)
                .find(|entry| u16::from_le_bytes([entry[0], entry[1]]) == tag)
                .unwrap();
            entry[4..8].copy_from_slice(&id.to_le_bytes());
            if id == UNDEFINED_ID {
                assert_eq!(PosixAcl::parse(&raw), Err(FsError::InvalidArgument));
            } else {
                assert!(PosixAcl::parse(&raw).is_ok());
            }
        }
    }
}

#[test]
fn a_matching_group_without_the_right_does_not_fall_through_to_other() {
    let acl = PosixAcl::from_text("u::rwx,g::---,m::rwx,o::r--");
    assert!(!allows(&acl, &creds(3000, &[GROUP]), AccessMode::Read));
    assert!(allows(&acl, &creds(3000, &[]), AccessMode::Read));
}

#[test]
fn incomplete_group_lists_never_widen_access() {
    let acl = PosixAcl::from_text("u::rwx,g::---,g:300:rwx,m::rwx,o::r--");
    let mut unknown = creds(3000, &[]);
    unknown.groups_complete = false;
    // The caller might be in the owning group, which grants nothing.
    assert!(!allows(&acl, &unknown, AccessMode::Read));
    let acl = PosixAcl::from_text("u::rwx,g::r--,g:300:r--,m::r--,o::r--");
    assert!(allows(&acl, &unknown, AccessMode::Read));
    assert!(!allows(&acl, &unknown, AccessMode::Write));
}

#[test]
fn combined_requests_need_one_entry_granting_every_bit() {
    let acl = PosixAcl::from_text("u::rwx,g::---,g:300:r--,g:301:-w-,m::rwx,o::---");
    let c = creds(3000, &[300, 301]);
    assert!(acl.permits_bits(OWNER, GROUP, &c, 4));
    assert!(acl.permits_bits(OWNER, GROUP, &c, 2));
    assert!(!acl.permits_bits(OWNER, GROUP, &c, 6));
}

#[test]
fn mode_is_the_source_of_owner_mask_and_other() {
    let stored = PosixAcl::from_text("u::rwx,u:2000:rwx,g::r-x,m::rwx,o::r-x");
    let (not_equiv, mode) = stored.equiv_mode();
    assert!(not_equiv);
    assert_eq!(mode, 0o775);
    // chmod 0640 narrows the mask to r-- and other to ---.
    let effective = stored.with_mode(0o640);
    assert_eq!(
        effective.to_text(),
        "u::rw-,u:2000:rwx,g::r-x,m::r--,o::---"
    );
    assert!(!allows(&effective, &creds(2000, &[]), AccessMode::Write));
    assert!(allows(&effective, &creds(2000, &[]), AccessMode::Read));
    // Without a mask, the group bits are the owning group's permissions.
    let minimal = PosixAcl::from_text("u::rw-,g::r--,o::r--");
    assert_eq!(minimal.equiv_mode(), (false, 0o644));
    assert_eq!(minimal.with_mode(0o600).to_text(), "u::rw-,g::---,o::---");
}

#[test]
fn create_masq_follows_linux() {
    let mut acl = PosixAcl::from_text("u::rwx,u:2000:rwx,g::r-x,m::rwx,o::r-x");
    let mut mode = 0o644;
    assert!(acl.create_masq(&mut mode));
    assert_eq!(mode, 0o644);
    assert_eq!(acl.to_text(), "u::rw-,u:2000:rwx,g::r-x,m::r--,o::r--");
    let mut minimal = PosixAcl::from_text("u::rwx,g::r-x,o::---");
    let mut mode = 0o777;
    assert!(!minimal.create_masq(&mut mode));
    assert_eq!(mode, 0o750);
}
