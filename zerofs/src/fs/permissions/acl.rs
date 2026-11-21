//! POSIX access control lists in the Linux `system.posix_acl_*` xattr format.
//!
//! The value layout is Linux `posix_acl_xattr`: a little-endian version word
//! (2) followed by eight-byte entries of tag, permission and id. Entries must be
//! in canonical order, which libacl and the kernel both produce.
//!
//! An access ACL never stores the owner, mask and other permissions as the
//! source of truth. Those come from the inode mode, the way Linux keeps
//! `i_mode` and the access ACL in step on chmod. ZeroFS therefore only rewrites
//! the mode when an access ACL is set, and builds the effective ACL from the
//! stored entries plus the current mode whenever it is read or evaluated.

use super::{AccessMode, Credentials};
use crate::fs::errors::FsError;
use bytes::Bytes;

pub(crate) const POSIX_ACL_ACCESS: &[u8] = b"system.posix_acl_access";
pub(crate) const POSIX_ACL_DEFAULT: &[u8] = b"system.posix_acl_default";

const ACL_EA_VERSION: u32 = 2;
const HEADER_LEN: usize = 4;
const ENTRY_LEN: usize = 8;
/// Linux `ACL_UNDEFINED_ID`, written for entries without a qualifier.
const UNDEFINED_ID: u32 = u32::MAX;

const TAG_USER_OBJ: u16 = 0x01;
const TAG_USER: u16 = 0x02;
const TAG_GROUP_OBJ: u16 = 0x04;
const TAG_GROUP: u16 = 0x08;
const TAG_MASK: u16 = 0x10;
const TAG_OTHER: u16 = 0x20;

#[cfg(test)]
const PERM_READ: u16 = 4;
#[cfg(test)]
const PERM_WRITE: u16 = 2;
#[cfg(test)]
const PERM_EXECUTE: u16 = 1;
const PERM_ALL: u16 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AclEntry {
    tag: u16,
    perm: u16,
    id: u32,
}

/// A validated POSIX ACL in canonical order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PosixAcl {
    entries: Vec<AclEntry>,
}

impl PosixAcl {
    /// Decode an xattr value. `Ok(None)` is the Linux removal encoding: an
    /// empty value or a header with no entries, which `posix_acl_from_xattr`
    /// turns into a NULL ACL.
    pub(crate) fn parse(value: &[u8]) -> Result<Option<Self>, FsError> {
        if value.is_empty() {
            return Ok(None);
        }
        if value.len() < HEADER_LEN {
            return Err(FsError::InvalidArgument);
        }
        let version = u32::from_le_bytes(value[..HEADER_LEN].try_into().unwrap());
        if version != ACL_EA_VERSION {
            // Linux returns EOPNOTSUPP for an unknown ACL xattr version.
            return Err(FsError::NotSupported);
        }
        let body = &value[HEADER_LEN..];
        if !body.len().is_multiple_of(ENTRY_LEN) {
            return Err(FsError::InvalidArgument);
        }
        if body.is_empty() {
            return Ok(None);
        }
        let (raw_entries, _) = body.as_chunks::<ENTRY_LEN>();
        let entries = raw_entries
            .iter()
            .map(|raw| {
                let tag = u16::from_le_bytes([raw[0], raw[1]]);
                let perm = u16::from_le_bytes([raw[2], raw[3]]);
                let id = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
                let id = match tag {
                    TAG_USER | TAG_GROUP => id,
                    TAG_USER_OBJ | TAG_GROUP_OBJ | TAG_MASK | TAG_OTHER => UNDEFINED_ID,
                    _ => return Err(FsError::InvalidArgument),
                };
                Ok(AclEntry { tag, perm, id })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let acl = Self { entries };
        acl.validate()?;
        Ok(Some(acl))
    }

    /// Linux `posix_acl_valid`: one owner, group and other entry, named
    /// entries in strictly increasing id order, and a mask whenever a named
    /// entry exists.
    fn validate(&self) -> Result<(), FsError> {
        #[derive(PartialEq)]
        enum State {
            UserObj,
            User,
            Group,
            Other,
            Done,
        }
        let mut state = State::UserObj;
        let mut needs_mask = false;
        let mut previous_id: Option<u32> = None;
        for entry in &self.entries {
            if entry.perm & !PERM_ALL != 0 {
                return Err(FsError::InvalidArgument);
            }
            match entry.tag {
                TAG_USER_OBJ if state == State::UserObj => {
                    state = State::User;
                    previous_id = None;
                }
                TAG_USER if state == State::User => {
                    if entry.id == UNDEFINED_ID
                        || previous_id.is_some_and(|previous| entry.id <= previous)
                    {
                        return Err(FsError::InvalidArgument);
                    }
                    previous_id = Some(entry.id);
                    needs_mask = true;
                }
                TAG_GROUP_OBJ if state == State::User => {
                    state = State::Group;
                    previous_id = None;
                }
                TAG_GROUP if state == State::Group => {
                    if entry.id == UNDEFINED_ID
                        || previous_id.is_some_and(|previous| entry.id <= previous)
                    {
                        return Err(FsError::InvalidArgument);
                    }
                    previous_id = Some(entry.id);
                    needs_mask = true;
                }
                TAG_MASK if state == State::Group => state = State::Other,
                TAG_OTHER if state == State::Other || (state == State::Group && !needs_mask) => {
                    state = State::Done;
                }
                _ => return Err(FsError::InvalidArgument),
            }
        }
        if state == State::Done {
            Ok(())
        } else {
            Err(FsError::InvalidArgument)
        }
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut out = Vec::with_capacity(HEADER_LEN + self.entries.len() * ENTRY_LEN);
        out.extend_from_slice(&ACL_EA_VERSION.to_le_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.tag.to_le_bytes());
            out.extend_from_slice(&entry.perm.to_le_bytes());
            out.extend_from_slice(&entry.id.to_le_bytes());
        }
        Bytes::from(out)
    }

    fn has_mask(&self) -> bool {
        self.entries.iter().any(|e| e.tag == TAG_MASK)
    }

    /// Linux `posix_acl_equiv_mode`. Returns whether the ACL carries more than
    /// the mode can express, and the permission bits (0o777) it implies.
    pub(crate) fn equiv_mode(&self) -> (bool, u32) {
        let mut mode = 0u32;
        let mut mask = None;
        let mut not_equiv = false;
        for entry in &self.entries {
            let perm = u32::from(entry.perm & PERM_ALL);
            match entry.tag {
                TAG_USER_OBJ => mode |= perm << 6,
                TAG_GROUP_OBJ => mode |= perm << 3,
                TAG_OTHER => mode |= perm,
                TAG_MASK => {
                    mask = Some(perm);
                    not_equiv = true;
                }
                _ => not_equiv = true,
            }
        }
        if let Some(mask) = mask {
            mode = (mode & !0o070) | (mask << 3);
        }
        (not_equiv, mode)
    }

    /// The effective access ACL for an inode: owner, mask (or owning group
    /// when there is no mask) and other permissions come from `mode`.
    pub(crate) fn with_mode(&self, mode: u32) -> Self {
        let has_mask = self.has_mask();
        let entries = self
            .entries
            .iter()
            .map(|entry| {
                let perm = match entry.tag {
                    TAG_USER_OBJ => (mode >> 6) as u16 & PERM_ALL,
                    TAG_MASK => (mode >> 3) as u16 & PERM_ALL,
                    TAG_GROUP_OBJ if !has_mask => (mode >> 3) as u16 & PERM_ALL,
                    TAG_OTHER => mode as u16 & PERM_ALL,
                    _ => entry.perm,
                };
                AclEntry { perm, ..*entry }
            })
            .collect();
        Self { entries }
    }

    /// Linux `posix_acl_create_masq`: restrict a default ACL copied to a new
    /// inode by the requested mode, and restrict that mode by the ACL. Returns
    /// whether the result needs to be stored as an access ACL.
    pub(crate) fn create_masq(&mut self, mode: &mut u32) -> bool {
        let mut not_equiv = false;
        let mut group_obj = None;
        let mut mask_obj = None;
        for (index, entry) in self.entries.iter_mut().enumerate() {
            match entry.tag {
                TAG_USER_OBJ => {
                    entry.perm &= ((*mode >> 6) & 0o7) as u16;
                    *mode &= (u32::from(entry.perm) << 6) | !0o700;
                }
                TAG_USER | TAG_GROUP => not_equiv = true,
                TAG_GROUP_OBJ => group_obj = Some(index),
                TAG_OTHER => {
                    entry.perm &= (*mode & 0o7) as u16;
                    *mode &= u32::from(entry.perm) | !0o007;
                }
                TAG_MASK => {
                    mask_obj = Some(index);
                    not_equiv = true;
                }
                _ => {}
            }
        }
        if let Some(index) = mask_obj.or(group_obj) {
            let entry = &mut self.entries[index];
            entry.perm &= ((*mode >> 3) & 0o7) as u16;
            *mode &= (u32::from(entry.perm) << 3) | !0o070;
        }
        not_equiv
    }

    /// Evaluate an effective access ACL (see [`Self::with_mode`]) for a caller
    /// who is neither root nor handled elsewhere. Follows Linux
    /// `posix_acl_permission` for callers whose groups are known.
    ///
    /// When the protocol did not supply the complete group list, the caller may
    /// belong to any group. The caller then receives only what the other entry
    /// and every group-class entry, limited by the mask, grant. This can deny
    /// what Linux would allow but never allows what Linux would deny.
    pub(crate) fn permits(
        &self,
        owner: u32,
        owning_group: u32,
        creds: &Credentials,
        mode: AccessMode,
    ) -> bool {
        self.permits_bits(owner, owning_group, creds, mode.bits())
    }

    /// [`Self::permits`] for a combined request of `r` (4), `w` (2) and `x`
    /// (1) bits. One entry must grant every requested bit, as in Linux.
    pub(crate) fn permits_bits(
        &self,
        owner: u32,
        owning_group: u32,
        creds: &Credentials,
        want: u16,
    ) -> bool {
        let grants = |perm: u16| perm & want == want;
        let mask = self
            .entries
            .iter()
            .find(|e| e.tag == TAG_MASK)
            .map_or(PERM_ALL, |e| e.perm);
        let other = self
            .entries
            .iter()
            .find(|e| e.tag == TAG_OTHER)
            .map_or(0, |e| e.perm);

        if creds.uid == owner {
            return self
                .entries
                .iter()
                .find(|e| e.tag == TAG_USER_OBJ)
                .is_some_and(|e| grants(e.perm));
        }
        if let Some(user) = self
            .entries
            .iter()
            .find(|e| e.tag == TAG_USER && e.id == creds.uid)
        {
            return grants(user.perm & mask);
        }

        let group_class = || {
            self.entries.iter().filter_map(|e| match e.tag {
                TAG_GROUP_OBJ => Some((owning_group, e.perm)),
                TAG_GROUP => Some((e.id, e.perm)),
                _ => None,
            })
        };
        let mut matched = false;
        for (gid, perm) in group_class() {
            if creds.is_known_member_of_group(gid) {
                if grants(perm & mask) {
                    return true;
                }
                matched = true;
            }
        }
        if matched {
            return false;
        }
        if !creds.groups_complete || creds.groups_truncated {
            return grants(other) && group_class().all(|(_, perm)| grants(perm & mask));
        }
        grants(other)
    }

    /// Build a canonical ACL from owner, named-user, owning-group,
    /// named-group and other permissions. A mask is added when named entries
    /// exist. Named ids must be distinct within each list.
    #[cfg(test)]
    pub(crate) fn from_parts(
        user_obj: u16,
        users: &[(u32, u16)],
        group_obj: u16,
        groups: &[(u32, u16)],
        mask: u16,
        other: u16,
    ) -> Self {
        let mut users = users.to_vec();
        users.sort_unstable();
        let mut groups = groups.to_vec();
        groups.sort_unstable();
        let mut entries = vec![AclEntry {
            tag: TAG_USER_OBJ,
            perm: user_obj,
            id: UNDEFINED_ID,
        }];
        entries.extend(users.iter().map(|&(id, perm)| AclEntry {
            tag: TAG_USER,
            perm,
            id,
        }));
        entries.push(AclEntry {
            tag: TAG_GROUP_OBJ,
            perm: group_obj,
            id: UNDEFINED_ID,
        });
        entries.extend(groups.iter().map(|&(id, perm)| AclEntry {
            tag: TAG_GROUP,
            perm,
            id,
        }));
        if !users.is_empty() || !groups.is_empty() {
            entries.push(AclEntry {
                tag: TAG_MASK,
                perm: mask,
                id: UNDEFINED_ID,
            });
        }
        entries.push(AclEntry {
            tag: TAG_OTHER,
            perm: other,
            id: UNDEFINED_ID,
        });
        Self { entries }
    }

    /// `getfacl`-style short text for test assertions.
    #[cfg(test)]
    pub(crate) fn to_text(&self) -> String {
        self.entries
            .iter()
            .map(|e| {
                let (kind, id) = match e.tag {
                    TAG_USER_OBJ => ("u", String::new()),
                    TAG_USER => ("u", e.id.to_string()),
                    TAG_GROUP_OBJ => ("g", String::new()),
                    TAG_GROUP => ("g", e.id.to_string()),
                    TAG_MASK => ("m", String::new()),
                    _ => ("o", String::new()),
                };
                let perm: String = [(PERM_READ, 'r'), (PERM_WRITE, 'w'), (PERM_EXECUTE, 'x')]
                    .iter()
                    .map(|&(bit, c)| if e.perm & bit != 0 { c } else { '-' })
                    .collect();
                format!("{kind}:{id}:{perm}")
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    #[cfg(test)]
    pub(crate) fn from_text(text: &str) -> Self {
        let entries = text
            .split(',')
            .map(|item| {
                let mut parts = item.split(':');
                let kind = parts.next().unwrap();
                let qualifier = parts.next().unwrap();
                let perms = parts.next().unwrap();
                let perm = perms.chars().fold(0u16, |acc, c| {
                    acc | match c {
                        'r' => PERM_READ,
                        'w' => PERM_WRITE,
                        'x' => PERM_EXECUTE,
                        '-' => 0,
                        _ => panic!("bad perm {c}"),
                    }
                });
                let (tag, id) = match (kind, qualifier.is_empty()) {
                    ("u", true) => (TAG_USER_OBJ, UNDEFINED_ID),
                    ("u", false) => (TAG_USER, qualifier.parse().unwrap()),
                    ("g", true) => (TAG_GROUP_OBJ, UNDEFINED_ID),
                    ("g", false) => (TAG_GROUP, qualifier.parse().unwrap()),
                    ("m", true) => (TAG_MASK, UNDEFINED_ID),
                    ("o", true) => (TAG_OTHER, UNDEFINED_ID),
                    _ => panic!("bad entry {item}"),
                };
                AclEntry { tag, perm, id }
            })
            .collect();
        let acl = Self { entries };
        acl.validate().expect("test ACL must be valid");
        acl
    }
}

#[cfg(test)]
mod tests;
