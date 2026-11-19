//! Mapping tests for the ACE patterns TrueNAS writes for SMB datasets: the
//! `NFS4_RESTRICTED`, `NFS4_OPEN` and `NFS4_HOME` presets, the SMB builtin
//! groups, inheritance flags, and deny entries.

use super::encode::*;
use super::{REPORT_ACL_OR_OWNER_CHANGE, REPORT_DELETE_FROM_PARENT, Refusal, map_nfs4_acl};

const OWNER: u32 = 1000;
const GROUP: u32 = 100;
/// TrueNAS maps the SMB `BUILTIN\Users` and `BUILTIN\Administrators` groups
/// to these local gids.
const BUILTIN_USERS: u32 = 545;
const BUILTIN_ADMINISTRATORS: u32 = 544;

fn map(entries: &[(u32, u32, u32, Who)], dir: bool) -> Result<super::Nfs4Mapping, Refusal> {
    map_nfs4_acl(&acl(entries), dir, OWNER, GROUP)
}

fn texts(m: &super::Nfs4Mapping) -> (String, Option<String>) {
    (m.access.to_text(), m.default.as_ref().map(|d| d.to_text()))
}

#[test]
fn restricted_preset_with_delete_child_on_a_directory() {
    let m = map(
        &[
            (ALLOW, FD, FULL_CONTROL, Who::Owner),
            (ALLOW, FD, MODIFY | DELETE_CHILD, Who::Group),
        ],
        true,
    )
    .unwrap();
    assert_eq!(
        texts(&m),
        (
            "u::rwx,g::rwx,o::---".into(),
            Some("u::rwx,g::rwx,o::---".into())
        )
    );
    assert!(m.reports.contains(REPORT_DELETE_FROM_PARENT));
}

#[test]
fn open_preset_with_delete_child_on_a_directory() {
    let m = map(
        &[
            (ALLOW, FD, FULL_CONTROL, Who::Owner),
            (ALLOW, FD, FULL_CONTROL, Who::Group),
            (ALLOW, FD, MODIFY | DELETE_CHILD, Who::Everyone),
        ],
        true,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::rwx,g::rwx,o::rwx");
    assert_eq!(texts(&m).1.as_deref(), Some("u::rwx,g::rwx,o::rwx"));
}

#[test]
fn home_preset_with_delete_child_and_traverse_for_everyone() {
    let m = map(
        &[
            (ALLOW, FD, FULL_CONTROL, Who::Owner),
            (ALLOW, FD, MODIFY | DELETE_CHILD, Who::Group),
            (ALLOW, FD, TRAVERSE, Who::Everyone),
        ],
        true,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::rwx,g::rwx,o::--x");
}

#[test]
fn smb_builtin_groups_become_named_group_entries() {
    let m = map(
        &[
            (ALLOW, FD, FULL_CONTROL, Who::Owner),
            (ALLOW, FD, FULL_CONTROL, Who::Group),
            (
                ALLOW,
                FD,
                MODIFY | DELETE_CHILD,
                Who::GroupId(BUILTIN_USERS),
            ),
            (
                ALLOW,
                FD,
                FULL_CONTROL,
                Who::GroupId(BUILTIN_ADMINISTRATORS),
            ),
        ],
        true,
    )
    .unwrap();
    assert_eq!(
        texts(&m).0,
        "u::rwx,g::rwx,g:544:rwx,g:545:rwx,m::rwx,o::---"
    );
    assert_eq!(
        texts(&m).1.as_deref(),
        Some("u::rwx,g::rwx,g:544:rwx,g:545:rwx,m::rwx,o::---")
    );
    // Non-owners lose WRITE_ACL and WRITE_OWNER; that is reported.
    assert!(m.reports.contains(REPORT_ACL_OR_OWNER_CHANGE));
}

#[test]
fn inherited_entries_on_files_map_to_an_access_acl() {
    let m = map(
        &[
            (ALLOW, INHERITED, FULL_CONTROL, Who::Owner),
            (ALLOW, INHERITED, MODIFY, Who::Group),
            (ALLOW, INHERITED, READ, Who::Everyone),
            (ALLOW, INHERITED, MODIFY, Who::User(2000)),
        ],
        false,
    )
    .unwrap();
    assert_eq!(
        texts(&m),
        ("u::rwx,u:2000:rwx,g::rwx,m::rwx,o::r-x".into(), None)
    );
    assert!(m.reports.contains(REPORT_DELETE_FROM_PARENT));
}

#[test]
fn inherit_only_entries_feed_only_the_default_acl() {
    let m = map(
        &[
            (ALLOW, 0, FULL_CONTROL, Who::Owner),
            (ALLOW, 0, READ, Who::Group),
            (ALLOW, FD | INHERIT_ONLY, FULL_CONTROL, Who::Owner),
            (
                ALLOW,
                FD | INHERIT_ONLY,
                MODIFY | DELETE_CHILD,
                Who::GroupId(BUILTIN_USERS),
            ),
        ],
        true,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::rwx,g::r-x,o::---");
    assert_eq!(
        texts(&m).1.as_deref(),
        Some("u::rwx,g::---,g:545:rwx,m::rwx,o::---")
    );
}

#[test]
fn a_directory_without_inheritable_entries_has_no_default() {
    let m = map(&[(ALLOW, 0, FULL_CONTROL, Who::Owner)], true).unwrap();
    assert_eq!(texts(&m), ("u::rwx,g::---,o::---".into(), None));
}

#[test]
fn expressible_deny_entries() {
    // A named user denied write ahead of an everyone@ grant.
    let m = map(
        &[
            (DENY, 0, WRITE_SET, Who::User(2000)),
            (ALLOW, 0, FULL_CONTROL, Who::Owner),
            (ALLOW, 0, MODIFY, Who::Everyone),
        ],
        false,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::rwx,u:2000:r-x,g::rwx,m::rwx,o::rwx");
    // An owner@ deny ahead of an everyone@ grant: the owner gets less than
    // everyone else, as on ZFS.
    let m = map(
        &[
            (DENY, 0, WRITE_SET, Who::Owner),
            (ALLOW, 0, MODIFY, Who::Everyone),
        ],
        false,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::r-x,g::rwx,o::rwx");
    // A deny for everyone@ after grants only removes what nothing granted.
    let m = map(
        &[
            (ALLOW, 0, FULL_CONTROL, Who::Owner),
            (ALLOW, 0, READ, Who::Group),
            (DENY, 0, FULL_CONTROL, Who::Everyone),
        ],
        false,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::rwx,g::r-x,o::---");
}

#[test]
fn group_deny_that_posix_cannot_express_is_refused() {
    // A member of both 300 and the owning group is denied write on ZFS but
    // POSIX would grant it through the owning-group entry.
    let refusal = map(
        &[
            (DENY, 0, WRITE_SET, Who::GroupId(300)),
            (ALLOW, 0, FULL_CONTROL, Who::Owner),
            (ALLOW, 0, MODIFY, Who::Group),
            (ALLOW, 0, READ, Who::GroupId(300)),
        ],
        false,
    )
    .unwrap_err();
    assert!(
        matches!(refusal, Refusal::DecisionMismatch { .. }),
        "{refusal}"
    );
}

#[test]
fn owner_rights_that_depend_on_group_membership_are_refused() {
    // On ZFS the owner gets write only while in the owning group; POSIX owner
    // permissions cannot depend on groups.
    let refusal = map(
        &[(ALLOW, 0, READ, Who::Owner), (ALLOW, 0, MODIFY, Who::Group)],
        false,
    )
    .unwrap_err();
    assert!(matches!(refusal, Refusal::DecisionMismatch { .. }));
}

#[test]
fn inheritance_patterns_posix_defaults_cannot_carry_are_refused() {
    for flags in [FILE_INHERIT, DIRECTORY_INHERIT] {
        assert_eq!(
            map(&[(ALLOW, flags, FULL_CONTROL, Who::Owner)], true).unwrap_err(),
            Refusal::PartialInheritance
        );
    }
    assert_eq!(
        map(
            &[(ALLOW, FD | NO_PROPAGATE, FULL_CONTROL, Who::Owner)],
            true
        )
        .unwrap_err(),
        Refusal::NoPropagateInheritance
    );
}

#[test]
fn partial_rights_are_refused() {
    assert_eq!(
        map(&[(ALLOW, 0, READ | WRITE_DATA, Who::Owner)], false).unwrap_err(),
        Refusal::PartialWrite
    );
    assert_eq!(
        map(&[(ALLOW, 0, READ_DATA, Who::Owner)], false).unwrap_err(),
        Refusal::DataWithoutNamedAttributes
    );
    // Directory write requires DELETE_CHILD.
    assert_eq!(
        map(
            &[
                (ALLOW, 0, FULL_CONTROL, Who::Owner),
                (ALLOW, 0, MODIFY, Who::Group)
            ],
            true
        )
        .unwrap_err(),
        Refusal::DirectoryWriteWithoutDelete
    );
    // DELETE_CHILD itself satisfies directory write.
    assert!(
        map(
            &[
                (ALLOW, 0, FULL_CONTROL, Who::Owner),
                (ALLOW, 0, MODIFY | DELETE_CHILD, Who::Group)
            ],
            true
        )
        .is_ok()
    );
}

#[test]
fn malformed_and_unsupported_values_are_refused() {
    let mut audit = acl(&[(ALLOW, 0, FULL_CONTROL, Who::Owner)]);
    audit[11] = 2;
    assert_eq!(
        map_nfs4_acl(&audit, false, OWNER, GROUP),
        Err(Refusal::UnsupportedAceType(2))
    );
    let mut short = acl(&[(ALLOW, 0, FULL_CONTROL, Who::Owner)]);
    short.pop();
    assert!(matches!(
        map_nfs4_acl(&short, false, OWNER, GROUP),
        Err(Refusal::Malformed(_))
    ));
    let mut extra_bits = acl(&[(ALLOW, 0, FULL_CONTROL, Who::Owner)]);
    extra_bits[20] = 0x80;
    assert!(matches!(
        map_nfs4_acl(&extra_bits, false, OWNER, GROUP),
        Err(Refusal::UnknownAccessBits(_))
    ));
    let mut bad_who = acl(&[(ALLOW, 0, FULL_CONTROL, Who::Owner)]);
    bad_who[27] = 9;
    assert!(matches!(
        map_nfs4_acl(&bad_who, false, OWNER, GROUP),
        Err(Refusal::Malformed(_))
    ));
}

#[test]
fn inherited_delete_does_not_authorize_deleting_every_child() {
    let entries = [
        (ALLOW, FD, FULL_CONTROL, Who::Owner),
        (ALLOW, FD, MODIFY, Who::Group),
    ];
    let source = super::parse(&acl(&entries)).unwrap();
    let caller = super::Caller {
        uid: 2000,
        groups: vec![GROUP],
    };
    let parent_rights = super::evaluate(&source, &caller, OWNER, GROUP);
    assert_ne!(parent_rights & WRITE_DATA, 0);
    assert_eq!(parent_rights & DELETE_CHILD, 0);
    // A child of a non-setgid directory may have another owning group.
    // Its group@ DELETE entry then does not authorize this caller.
    assert_eq!(
        super::evaluate(&source, &caller, OWNER, GROUP + 1) & super::DELETE,
        0
    );
    assert_eq!(
        map(&entries, true).unwrap_err(),
        Refusal::DirectoryWriteWithoutDelete
    );
    // Applying the same unsafe grant only to future directories is refused too.
    assert_eq!(
        map(
            &[
                (ALLOW, 0, FULL_CONTROL, Who::Owner),
                (ALLOW, FD | INHERIT_ONLY, FULL_CONTROL, Who::Owner),
                (ALLOW, FD | INHERIT_ONLY, MODIFY, Who::Group),
            ],
            true,
        )
        .unwrap_err(),
        Refusal::DirectoryWriteWithoutDelete
    );
    // A file's MODIFY rights remain representable.
    assert!(map(&entries, false).is_ok());
}

#[test]
fn unknown_identity_flags_and_undefined_named_ids_are_refused() {
    for iflag in [3u32, 0x8000_0000, 0x8000_0001] {
        let mut value = acl(&[(ALLOW, 0, FULL_CONTROL, Who::Owner)]);
        value[16..20].copy_from_slice(&iflag.to_be_bytes());
        assert_eq!(
            map_nfs4_acl(&value, false, OWNER, GROUP),
            Err(Refusal::Malformed("iflag"))
        );
    }
    for principal in [Who::User(u32::MAX), Who::GroupId(u32::MAX)] {
        assert_eq!(
            map(&[(ALLOW, 0, READ, principal)], false),
            Err(Refusal::Malformed("undefined principal id"))
        );
    }
    for principal in [Who::User(u32::MAX - 1), Who::GroupId(u32::MAX - 1)] {
        // Keep the owner's rights independent of named-group membership so
        // this control tests the high ID, not an unrepresentable ACL policy.
        assert!(
            map(
                &[
                    (ALLOW, 0, FULL_CONTROL, Who::Owner),
                    (ALLOW, 0, READ, principal),
                ],
                false,
            )
            .is_ok()
        );
    }
}

#[test]
fn named_users_who_may_own_future_children_are_checked() {
    // user:2000 gets READ; owner@ gets FULL. A child owned by 2000 would get
    // the union on ZFS (FULL) and the owner entry (FULL) in POSIX, so it maps.
    let m = map(
        &[
            (ALLOW, FD, FULL_CONTROL, Who::Owner),
            (ALLOW, FD, READ, Who::User(2000)),
        ],
        true,
    )
    .unwrap();
    assert_eq!(
        texts(&m).1.as_deref(),
        Some("u::rwx,u:2000:r-x,g::---,m::r-x,o::---")
    );
    // A deny for user:2000 ahead of owner@ would leave a child that 2000 owns
    // without write on ZFS, while POSIX gives the owner entry. Refused.
    let refusal = map(
        &[
            (DENY, FD, WRITE_SET, Who::User(2000)),
            (ALLOW, FD, FULL_CONTROL, Who::Owner),
        ],
        true,
    )
    .unwrap_err();
    assert!(matches!(refusal, Refusal::DecisionMismatch { .. }));
}

#[test]
fn too_many_principals_are_refused() {
    let entries: Vec<_> = (0..7)
        .map(|i| (ALLOW, 0, READ, Who::GroupId(5000 + i)))
        .collect();
    assert_eq!(
        map(&entries, false).unwrap_err(),
        Refusal::TooManyPrincipals
    );
}

#[test]
fn read_data_and_execute_only_entries() {
    let m = map(
        &[
            (ALLOW, 0, FULL_CONTROL, Who::Owner),
            (ALLOW, 0, TRAVERSE, Who::Group),
            (ALLOW, 0, READ & !EXECUTE, Who::Everyone),
        ],
        false,
    )
    .unwrap();
    assert_eq!(texts(&m).0, "u::rwx,g::r-x,o::r--");
}
