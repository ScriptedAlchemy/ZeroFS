//! Translation of OpenZFS NFSv4 ACLs (`system.nfs4_acl_xdr`) into POSIX ACLs.
//!
//! A translation is accepted only when the POSIX ACL makes the same read,
//! write and execute decision as the NFSv4 ACL for every class of caller.
//! The check enumerates every caller class the ACL can distinguish: the
//! owner, each named user, an unnamed user, and every combination of
//! membership in the owning group and the named groups. For a directory's
//! inheritable entries it repeats the check with the future child's owner and
//! group set to each named principal and to an unnamed one, for both file
//! and directory children. Anything that fails returns a [`Refusal`]; nothing
//! is approximated. `docs/permissions-and-acl-contract.md` lists the rules.
//!
//! The value layout is the one OpenZFS `zpl_xattr.c` emits: big-endian
//! `acl_flags`, `ace_count`, then per entry `type`, `flags`, `iflag`,
//! `access_mask` and `who`. `iflag` 1 marks `who` as 1 (owner@), 2 (group@)
//! or 3 (everyone@).

use super::Credentials;
use super::acl::PosixAcl;
use std::collections::BTreeSet;
use std::fmt;

const ALLOW: u32 = 0;
const DENY: u32 = 1;

const FILE_INHERIT: u32 = 0x1;
const DIRECTORY_INHERIT: u32 = 0x2;
const NO_PROPAGATE: u32 = 0x4;
const INHERIT_ONLY: u32 = 0x8;
const IDENTIFIER_GROUP: u32 = 0x40;
const INHERITED: u32 = 0x80;
const KNOWN_FLAGS: u32 =
    FILE_INHERIT | DIRECTORY_INHERIT | NO_PROPAGATE | INHERIT_ONLY | IDENTIFIER_GROUP | INHERITED;

const SPECIAL_WHO: u32 = 1;
const WHO_OWNER: u32 = 1;
const WHO_GROUP: u32 = 2;
const WHO_EVERYONE: u32 = 3;

const READ_DATA: u32 = 0x1;
const WRITE_DATA: u32 = 0x2;
const APPEND_DATA: u32 = 0x4;
const READ_NAMED_ATTRS: u32 = 0x8;
const WRITE_NAMED_ATTRS: u32 = 0x10;
const EXECUTE: u32 = 0x20;
const DELETE_CHILD: u32 = 0x40;
const READ_ATTRIBUTES: u32 = 0x80;
const WRITE_ATTRIBUTES: u32 = 0x100;
const DELETE: u32 = 0x10000;
const READ_ACL: u32 = 0x20000;
const WRITE_ACL: u32 = 0x40000;
const WRITE_OWNER: u32 = 0x80000;
const SYNCHRONIZE: u32 = 0x100000;
const KNOWN_MASK: u32 = READ_DATA
    | WRITE_DATA
    | APPEND_DATA
    | READ_NAMED_ATTRS
    | WRITE_NAMED_ATTRS
    | EXECUTE
    | DELETE_CHILD
    | READ_ATTRIBUTES
    | WRITE_ATTRIBUTES
    | DELETE
    | READ_ACL
    | WRITE_ACL
    | WRITE_OWNER
    | SYNCHRONIZE;

/// More entries than OpenZFS writes for any preset; also bounds the check.
const MAX_ACES: usize = 128;
/// Bounds on named principals. The membership enumeration is exponential in
/// the number of groups.
const MAX_NAMED_USERS: usize = 8;
const MAX_NAMED_GROUPS: usize = 6;

/// Why an NFSv4 ACL cannot be enforced as a POSIX ACL without changing an
/// access decision. Each variant names one rule in the contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    Malformed(&'static str),
    UnsupportedAceType(u32),
    UnknownFlags(u32),
    UnknownAccessBits(u32),
    TooManyEntries,
    TooManyPrincipals,
    /// An inheritable entry lacks file or directory inheritance, so files and
    /// subdirectories would inherit different ACLs.
    PartialInheritance,
    /// `NO_PROPAGATE_INHERIT`: grandchildren would not inherit. POSIX default
    /// ACLs always propagate.
    NoPropagateInheritance,
    /// Grants `WRITE_DATA` without `APPEND_DATA` or the reverse, or on a
    /// directory `ADD_FILE` without `ADD_SUBDIRECTORY`.
    PartialWrite,
    /// Directory write without `DELETE_CHILD` and without inherited `DELETE`.
    DirectoryWriteWithoutDelete,
    /// Grants read or write data without the matching named-attribute right.
    DataWithoutNamedAttributes,
    /// Grants write data without `WRITE_ATTRIBUTES`; POSIX write lets the
    /// caller set times to now.
    WriteWithoutWriteAttributes,
    /// A caller class would get a different read, write or execute decision.
    DecisionMismatch {
        caller: String,
        nfs4: u16,
        posix: u16,
    },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(why) => write!(f, "malformed_xdr: {why}"),
            Self::UnsupportedAceType(t) => write!(f, "unsupported_ace_type: {t}"),
            Self::UnknownFlags(x) => write!(f, "unknown_ace_flags: {x:#x}"),
            Self::UnknownAccessBits(x) => write!(f, "unknown_access_bits: {x:#x}"),
            Self::TooManyEntries => write!(f, "too_many_entries"),
            Self::TooManyPrincipals => write!(f, "too_many_named_principals"),
            Self::PartialInheritance => write!(f, "file_or_directory_only_inheritance"),
            Self::NoPropagateInheritance => write!(f, "no_propagate_inheritance"),
            Self::PartialWrite => write!(f, "partial_write_rights"),
            Self::DirectoryWriteWithoutDelete => {
                write!(f, "directory_write_without_delete_child")
            }
            Self::DataWithoutNamedAttributes => {
                write!(f, "data_right_without_named_attribute_right")
            }
            Self::WriteWithoutWriteAttributes => write!(f, "write_without_write_attributes"),
            Self::DecisionMismatch {
                caller,
                nfs4,
                posix,
            } => write!(
                f,
                "decision_mismatch: caller {caller} nfs4 rwx {nfs4:03b} posix rwx {posix:03b}"
            ),
        }
    }
}

/// Narrowings the translation accepted. Each is a right POSIX ACLs cannot
/// grant per file; the caller loses it, or for directory deletion gains it
/// only through the reported inherited `DELETE`.
pub(crate) const REPORT_ACL_OR_OWNER_CHANGE: &str = "acl_or_owner_change_limited_to_owner";
pub(crate) const REPORT_DELETE_FROM_PARENT: &str = "delete_follows_parent_directory_write";
pub(crate) const REPORT_ATTRIBUTE_WRITE_DROPPED: &str =
    "attribute_write_without_data_write_dropped";
pub(crate) const REPORT_CHILD_DELETE_VIA_INHERITED: &str =
    "child_deletion_relies_on_inherited_delete";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Nfs4Mapping {
    pub access: PosixAcl,
    /// The inheritable entries as a default ACL, or `None` when the directory
    /// has none. Always `None` for non-directories.
    pub default: Option<PosixAcl>,
    pub reports: BTreeSet<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Who {
    Owner,
    Group,
    Everyone,
    User(u32),
    GroupId(u32),
}

#[derive(Debug, Clone, Copy)]
struct Ace {
    allow: bool,
    flags: u32,
    mask: u32,
    who: Who,
}

fn parse(value: &[u8]) -> Result<Vec<Ace>, Refusal> {
    if value.len() < 8 || !value.len().is_multiple_of(4) {
        return Err(Refusal::Malformed("length"));
    }
    let word = |i: usize| u32::from_be_bytes(value[i * 4..i * 4 + 4].try_into().unwrap());
    // Word 0 is acl_flags (auto-inherit, protected, defaulted). They steer
    // Windows-side propagation, not access, and are not kept.
    let count = word(1) as usize;
    if (value.len() - 8) != count.saturating_mul(20) {
        return Err(Refusal::Malformed("entry count"));
    }
    if count > MAX_ACES {
        return Err(Refusal::TooManyEntries);
    }
    (0..count)
        .map(|i| {
            let base = 2 + i * 5;
            let (kind, flags, iflag, mask, who) = (
                word(base),
                word(base + 1),
                word(base + 2),
                word(base + 3),
                word(base + 4),
            );
            let allow = match kind {
                ALLOW => true,
                DENY => false,
                other => return Err(Refusal::UnsupportedAceType(other)),
            };
            if flags & !KNOWN_FLAGS != 0 {
                return Err(Refusal::UnknownFlags(flags & !KNOWN_FLAGS));
            }
            if mask & !KNOWN_MASK != 0 {
                return Err(Refusal::UnknownAccessBits(mask & !KNOWN_MASK));
            }
            let who = if iflag & SPECIAL_WHO != 0 {
                match who {
                    WHO_OWNER => Who::Owner,
                    WHO_GROUP => Who::Group,
                    WHO_EVERYONE => Who::Everyone,
                    _ => return Err(Refusal::Malformed("special who")),
                }
            } else if iflag != 0 {
                return Err(Refusal::Malformed("iflag"));
            } else if flags & IDENTIFIER_GROUP != 0 {
                Who::GroupId(who)
            } else {
                Who::User(who)
            };
            Ok(Ace {
                allow,
                flags,
                mask,
                who,
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
struct Caller {
    uid: u32,
    groups: Vec<u32>,
}

impl Caller {
    fn credentials(&self, unrelated_gid: u32) -> Credentials {
        let mut groups = [0u32; 16];
        groups[..self.groups.len()].copy_from_slice(&self.groups);
        Credentials {
            uid: self.uid,
            gid: unrelated_gid,
            gid_known: true,
            groups,
            groups_count: self.groups.len(),
            groups_complete: true,
            groups_truncated: false,
        }
    }
}

/// The rights an ordered NFSv4 ACL allows a caller: each bit is decided by
/// the first matching entry that mentions it, and undecided bits are denied.
fn evaluate(aces: &[Ace], caller: &Caller, owner: u32, group: u32) -> u32 {
    let mut decided = 0;
    let mut allowed = 0;
    for ace in aces {
        let matches = match ace.who {
            Who::Owner => caller.uid == owner,
            Who::Group => caller.groups.contains(&group),
            Who::Everyone => true,
            Who::User(uid) => caller.uid == uid,
            Who::GroupId(gid) => caller.groups.contains(&gid),
        };
        if !matches {
            continue;
        }
        let bits = ace.mask & !decided;
        if ace.allow {
            allowed |= bits;
        }
        decided |= bits;
    }
    allowed
}

/// POSIX `rwx` for NFSv4 rights, or the rule that prevents it.
fn rwx(
    allowed: u32,
    is_directory: bool,
    is_owner: bool,
    inherited_delete: impl FnOnce() -> bool,
    reports: &mut BTreeSet<&'static str>,
) -> Result<u16, Refusal> {
    let read = allowed & READ_DATA != 0;
    let execute = allowed & EXECUTE != 0;
    let (first, second) = (allowed & WRITE_DATA != 0, allowed & APPEND_DATA != 0);
    if first != second {
        return Err(Refusal::PartialWrite);
    }
    let write = first;
    if read && allowed & READ_NAMED_ATTRS == 0 || write && allowed & WRITE_NAMED_ATTRS == 0 {
        return Err(Refusal::DataWithoutNamedAttributes);
    }
    if write && allowed & WRITE_ATTRIBUTES == 0 {
        return Err(Refusal::WriteWithoutWriteAttributes);
    }
    if is_directory && write && allowed & DELETE_CHILD == 0 {
        if inherited_delete() {
            reports.insert(REPORT_CHILD_DELETE_VIA_INHERITED);
        } else {
            return Err(Refusal::DirectoryWriteWithoutDelete);
        }
    }
    if !is_owner {
        if !write && allowed & (WRITE_NAMED_ATTRS | WRITE_ATTRIBUTES) != 0 {
            reports.insert(REPORT_ATTRIBUTE_WRITE_DROPPED);
        }
        if allowed & (WRITE_ACL | WRITE_OWNER) != 0 {
            reports.insert(REPORT_ACL_OR_OWNER_CHANGE);
        }
    }
    if allowed & DELETE != 0 {
        reports.insert(REPORT_DELETE_FROM_PARENT);
    }
    Ok(u16::from(read) << 2 | u16::from(write) << 1 | u16::from(execute))
}

struct Principals {
    users: Vec<u32>,
    groups: Vec<u32>,
}

impl Principals {
    fn of(aces: &[Ace]) -> Result<Self, Refusal> {
        let mut users = BTreeSet::new();
        let mut groups = BTreeSet::new();
        for ace in aces {
            match ace.who {
                Who::User(uid) => {
                    users.insert(uid);
                }
                Who::GroupId(gid) => {
                    groups.insert(gid);
                }
                _ => {}
            }
        }
        if users.len() > MAX_NAMED_USERS || groups.len() > MAX_NAMED_GROUPS {
            return Err(Refusal::TooManyPrincipals);
        }
        Ok(Self {
            users: users.into_iter().collect(),
            groups: groups.into_iter().collect(),
        })
    }

    /// An id outside `used`, for an unnamed principal.
    fn fresh(used: &[u32]) -> u32 {
        (0..u32::MAX)
            .rev()
            .skip(2)
            .find(|id| !used.contains(id))
            .expect("fewer named ids than u32 values")
    }
}

/// One way an ACL is applied: to an existing inode, or to a future child.
struct Scenario {
    owner: u32,
    group: u32,
    is_directory: bool,
}

/// The POSIX ACL equivalent to `aces` under `scenario`, before verification.
fn build(
    aces: &[Ace],
    inherited: &[Ace],
    principals: &Principals,
    scenario: &Scenario,
    stranger: u32,
    reports: &mut BTreeSet<&'static str>,
) -> Result<PosixAcl, Refusal> {
    let eval = |caller: &Caller| evaluate(aces, caller, scenario.owner, scenario.group);
    let inherited_delete = |caller: &Caller| {
        // A child's owner is whoever creates it, so owner@ cannot be assumed
        // to match; the child's group is assumed to be this directory's.
        let child_owner =
            Principals::fresh(&[&[caller.uid, scenario.owner][..], &principals.users[..]].concat());
        evaluate(inherited, caller, child_owner, scenario.group) & DELETE != 0
    };
    let perm = |caller: Caller, is_owner: bool, reports: &mut BTreeSet<&'static str>| {
        rwx(
            eval(&caller),
            scenario.is_directory,
            is_owner,
            || inherited_delete(&caller),
            reports,
        )
    };
    let user_obj = perm(
        Caller {
            uid: scenario.owner,
            groups: vec![],
        },
        true,
        reports,
    )?;
    let mut users = Vec::new();
    for &uid in principals.users.iter().filter(|&&u| u != scenario.owner) {
        users.push((
            uid,
            perm(
                Caller {
                    uid,
                    groups: vec![],
                },
                false,
                reports,
            )?,
        ));
    }
    let group_obj = perm(
        Caller {
            uid: stranger,
            groups: vec![scenario.group],
        },
        false,
        reports,
    )?;
    let mut groups = Vec::new();
    for &gid in principals.groups.iter().filter(|&&g| g != scenario.group) {
        groups.push((
            gid,
            perm(
                Caller {
                    uid: stranger,
                    groups: vec![gid],
                },
                false,
                reports,
            )?,
        ));
    }
    let other = perm(
        Caller {
            uid: stranger,
            groups: vec![],
        },
        false,
        reports,
    )?;
    let mask = users
        .iter()
        .chain(groups.iter())
        .fold(group_obj, |mask, &(_, perm)| mask | perm);
    Ok(PosixAcl::from_parts(
        user_obj, &users, group_obj, &groups, mask, other,
    ))
}

/// Check that `acl` (with owner `scenario.owner`) decides every request the
/// way the NFSv4 entries do, for every caller class.
fn verify(
    acl: &PosixAcl,
    aces: &[Ace],
    inherited: &[Ace],
    principals: &Principals,
    scenario: &Scenario,
) -> Result<(), Refusal> {
    let mut uids: Vec<u32> = principals.users.clone();
    uids.push(scenario.owner);
    uids.sort_unstable();
    uids.dedup();
    let stranger = Principals::fresh(&uids);
    uids.push(stranger);
    let mut gids: Vec<u32> = principals.groups.clone();
    gids.push(scenario.group);
    gids.sort_unstable();
    gids.dedup();
    let unrelated_gid = Principals::fresh(&gids);
    let mut scratch = BTreeSet::new();
    for &uid in &uids {
        for subset in 0u32..(1 << gids.len()) {
            let caller = Caller {
                uid,
                groups: gids
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| subset & (1 << i) != 0)
                    .map(|(_, &g)| g)
                    .collect(),
            };
            let allowed = evaluate(aces, &caller, scenario.owner, scenario.group);
            let child_owner = Principals::fresh(&[&uids[..], &[uid][..]].concat());
            let nfs4 = rwx(
                allowed,
                scenario.is_directory,
                uid == scenario.owner,
                || evaluate(inherited, &caller, child_owner, scenario.group) & DELETE != 0,
                &mut scratch,
            )?;
            let creds = caller.credentials(unrelated_gid);
            let mut posix = 0;
            for bit in [4u16, 2, 1] {
                if acl.permits_bits(scenario.owner, scenario.group, &creds, bit) {
                    posix |= bit;
                }
            }
            // Combined requests must agree too: POSIX needs one entry to
            // grant all requested bits.
            for want in 1u16..=7 {
                let nfs4_grants = nfs4 & want == want;
                if acl.permits_bits(scenario.owner, scenario.group, &creds, want) != nfs4_grants {
                    return Err(Refusal::DecisionMismatch {
                        caller: format!("uid {uid} groups {:?}", caller.groups),
                        nfs4,
                        posix,
                    });
                }
            }
        }
    }
    Ok(())
}

/// Translate an `system.nfs4_acl_xdr` value for an inode owned by
/// `owner:group`.
pub(crate) fn map_nfs4_acl(
    value: &[u8],
    is_directory: bool,
    owner: u32,
    group: u32,
) -> Result<Nfs4Mapping, Refusal> {
    let aces = parse(value)?;
    // Bounds the enumeration and supplies ids no entry names.
    let named = Principals::of(&aces)?;
    let mut reports = BTreeSet::new();

    let (effective, inheritable): (Vec<Ace>, Vec<Ace>) = if is_directory {
        let effective = aces
            .iter()
            .filter(|a| a.flags & INHERIT_ONLY == 0)
            .copied()
            .collect();
        let mut inheritable = Vec::new();
        for ace in &aces {
            let inherit = ace.flags & (FILE_INHERIT | DIRECTORY_INHERIT);
            if inherit == 0 {
                continue;
            }
            if inherit != FILE_INHERIT | DIRECTORY_INHERIT {
                return Err(Refusal::PartialInheritance);
            }
            if ace.flags & NO_PROPAGATE != 0 {
                return Err(Refusal::NoPropagateInheritance);
            }
            inheritable.push(Ace {
                flags: ace.flags & IDENTIFIER_GROUP,
                ..*ace
            });
        }
        (effective, inheritable)
    } else {
        // Inheritance flags have no effect on a file; inherit-only entries
        // never apply to it.
        (
            aces.iter()
                .filter(|a| a.flags & INHERIT_ONLY == 0)
                .copied()
                .collect(),
            Vec::new(),
        )
    };

    let all_ids: Vec<u32> = [&named.users[..], &named.groups[..], &[owner, group][..]].concat();
    // Each ACL names only the principals its own entries mention.
    let principals = Principals::of(&effective)?;
    let stranger = Principals::fresh(&all_ids);
    let here = Scenario {
        owner,
        group,
        is_directory,
    };
    let access = build(
        &effective,
        &inheritable,
        &principals,
        &here,
        stranger,
        &mut reports,
    )?;
    verify(&access, &effective, &inheritable, &principals, &here)?;

    let default = if inheritable.is_empty() {
        None
    } else {
        // Build with an unnamed future owner and group, then verify for every
        // owner and group a child could have that the ACL can tell apart.
        let principals = Principals::of(&inheritable)?;
        let fresh_owner = Principals::fresh(&all_ids);
        let fresh_group = Principals::fresh(&[&all_ids[..], &[fresh_owner][..]].concat());
        let construct = Scenario {
            owner: fresh_owner,
            group: fresh_group,
            is_directory: false,
        };
        let stranger = Principals::fresh(&[&all_ids[..], &[fresh_owner, fresh_group][..]].concat());
        let acl = build(
            &inheritable,
            &inheritable,
            &principals,
            &construct,
            stranger,
            &mut reports,
        )?;
        let owners = std::iter::once(fresh_owner).chain(principals.users.iter().copied());
        for child_owner in owners {
            let groups = std::iter::once(fresh_group).chain(principals.groups.iter().copied());
            for child_group in groups {
                for child_is_directory in [false, true] {
                    let scenario = Scenario {
                        owner: child_owner,
                        group: child_group,
                        is_directory: child_is_directory,
                    };
                    verify(&acl, &inheritable, &inheritable, &principals, &scenario)?;
                    // Directory rules may add reports the file build skipped.
                    if child_is_directory {
                        build(
                            &inheritable,
                            &inheritable,
                            &principals,
                            &scenario,
                            Principals::fresh(
                                &[&all_ids[..], &[child_owner, child_group][..]].concat(),
                            ),
                            &mut reports,
                        )?;
                    }
                }
            }
        }
        Some(acl)
    };
    Ok(Nfs4Mapping {
        access,
        default,
        reports,
    })
}

/// Encode entries in the OpenZFS layout. Used by tests and fixtures.
#[cfg(test)]
pub(crate) mod encode {
    pub const ALLOW: u32 = super::ALLOW;
    pub const DENY: u32 = super::DENY;
    pub const FD: u32 = super::FILE_INHERIT | super::DIRECTORY_INHERIT;
    pub const FILE_INHERIT: u32 = super::FILE_INHERIT;
    pub const DIRECTORY_INHERIT: u32 = super::DIRECTORY_INHERIT;
    pub const NO_PROPAGATE: u32 = super::NO_PROPAGATE;
    pub const INHERIT_ONLY: u32 = super::INHERIT_ONLY;
    pub const INHERITED: u32 = super::INHERITED;

    /// OpenZFS and TrueNAS permission sets.
    pub const READ_SET: u32 =
        super::READ_DATA | super::READ_NAMED_ATTRS | super::READ_ATTRIBUTES | super::READ_ACL;
    pub const WRITE_SET: u32 =
        super::WRITE_DATA | super::APPEND_DATA | super::WRITE_NAMED_ATTRS | super::WRITE_ATTRIBUTES;
    /// TrueNAS BASIC `TRAVERSE`.
    pub const TRAVERSE: u32 = super::EXECUTE
        | super::READ_NAMED_ATTRS
        | super::READ_ATTRIBUTES
        | super::READ_ACL
        | super::SYNCHRONIZE;
    /// TrueNAS BASIC `READ`.
    pub const READ: u32 = READ_SET | super::EXECUTE | super::SYNCHRONIZE;
    /// TrueNAS BASIC `MODIFY`: everything except `DELETE_CHILD`, `WRITE_ACL`
    /// and `WRITE_OWNER`.
    pub const MODIFY: u32 = READ | WRITE_SET | super::DELETE;
    /// TrueNAS BASIC `FULL_CONTROL`, OpenZFS `full_set`.
    pub const FULL_CONTROL: u32 = super::KNOWN_MASK;
    pub const WRITE_DATA: u32 = super::WRITE_DATA;
    pub const READ_DATA: u32 = super::READ_DATA;
    pub const EXECUTE: u32 = super::EXECUTE;
    pub const DELETE_CHILD: u32 = super::DELETE_CHILD;

    #[derive(Clone, Copy)]
    pub enum Who {
        Owner,
        Group,
        Everyone,
        User(u32),
        GroupId(u32),
    }

    pub fn acl(entries: &[(u32, u32, u32, Who)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for &(kind, flags, mask, who) in entries {
            let (flags, iflag, id) = match who {
                Who::Owner => (flags, 1, 1),
                Who::Group => (flags | super::IDENTIFIER_GROUP, 1, 2),
                Who::Everyone => (flags, 1, 3),
                Who::User(uid) => (flags, 0, uid),
                Who::GroupId(gid) => (flags | super::IDENTIFIER_GROUP, 0, gid),
            };
            for word in [kind, flags, iflag, mask, id] {
                out.extend_from_slice(&word.to_be_bytes());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests;
