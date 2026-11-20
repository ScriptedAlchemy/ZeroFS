//! Room the pool has promised to segment data it has not placed yet.
//!
//! A WRITE is admitted before its reply: it takes a [`Claim`] on one member
//! for an upper bound of the bytes its frames, their directory entries and a
//! segment footer can occupy. Its frames then land in an open segment, whose
//! lease on the same member grows by what the claim funds. The lease stays
//! until the segment's placement commits, so an admitted segment always has
//! room for its seal, whether it is published at once or later by the
//! writeback journal. Every reservation and every new claim counts the
//! outstanding claims and leases, so no two promises share the same bytes.
//!
//! Nothing here is durable. A restart forgets every claim and lease, with the
//! unflushed data they covered; journaled segments are leased again when the
//! writeback store attaches to the pool.
use super::PoolMember;
use crate::segment::Segid;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Key {
    Claim(u64),
    Segment(Segid),
}

#[derive(Debug)]
struct Entry {
    member: String,
    bytes: u64,
    /// A catalog reservation for the segment is outstanding and already
    /// counts these bytes.
    placing: bool,
}

/// Promised bytes on one member: logical bytes against its declared capacity
/// and their physical cost against its filesystem.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Promise {
    pub(super) logical: u64,
    pub(super) physical: u64,
}

/// Every member's outstanding promises.
#[derive(Clone, Debug, Default)]
pub(super) struct Admitted(BTreeMap<String, Promise>);

impl Admitted {
    pub(super) fn on(&self, member: &str) -> Promise {
        self.0.get(member).copied().unwrap_or_default()
    }

    /// Physical bytes promised across `members` that are local. Local members
    /// share one physical budget in the allocator, so their promises do too.
    pub(super) fn local_physical(&self, members: &[PoolMember]) -> u64 {
        members
            .iter()
            .filter(|member| member.is_local())
            .fold(0_u64, |sum, member| {
                sum.saturating_add(self.on(&member.id).physical)
            })
    }
}

#[derive(Debug, Default)]
pub(super) struct Ledger {
    entries: HashMap<Key, Entry>,
    next_claim: u64,
}

impl Ledger {
    /// Promises outside `except` that no catalog reservation counts yet.
    pub(super) fn admitted(&self, members: &[PoolMember], except: Option<Key>) -> Admitted {
        let mut admitted = BTreeMap::<String, Promise>::new();
        for (key, entry) in &self.entries {
            if entry.placing || Some(*key) == except {
                continue;
            }
            let Some(member) = members.iter().find(|member| member.id == entry.member) else {
                continue;
            };
            let promise = admitted.entry(entry.member.clone()).or_default();
            promise.logical = promise.logical.saturating_add(entry.bytes);
            promise.physical = promise
                .physical
                .saturating_add(member.put_cost(entry.bytes));
        }
        Admitted(admitted)
    }

    pub(super) fn member_of(&self, key: Key) -> Option<&str> {
        self.entries.get(&key).map(|entry| entry.member.as_str())
    }

    pub(super) fn insert_claim(&mut self, member: String, bytes: u64) -> u64 {
        let id = self.next_claim;
        self.next_claim += 1;
        self.entries.insert(
            Key::Claim(id),
            Entry {
                member,
                bytes,
                placing: false,
            },
        );
        id
    }

    /// Lease `segment` for `bytes` on `member` unless it is already leased.
    pub(super) fn insert_lease(&mut self, segment: Segid, member: String, bytes: u64) {
        self.entries.entry(Key::Segment(segment)).or_insert(Entry {
            member,
            bytes,
            placing: false,
        });
    }

    pub(super) fn set_placing(&mut self, segment: Segid, placing: bool) {
        if let Some(entry) = self.entries.get_mut(&Key::Segment(segment)) {
            entry.placing = placing;
        }
    }

    pub(super) fn remove(&mut self, key: Key) {
        self.entries.remove(&key);
    }

    /// Grow `segment`'s lease to `total` bytes, paid from `claim`. The lease
    /// is created on the claim's member. `Err` means the segment is leased on
    /// another member, so these bytes must start a new segment instead.
    fn fund(&mut self, claim: u64, segment: Segid, total: u64) -> Result<(), MemberMismatch> {
        let member = match self.entries.get(&Key::Claim(claim)) {
            Some(entry) => entry.member.clone(),
            None => return Err(MemberMismatch),
        };
        let grown = match self.entries.get_mut(&Key::Segment(segment)) {
            Some(lease) if lease.member != member => return Err(MemberMismatch),
            Some(lease) => {
                let grown = total.saturating_sub(lease.bytes);
                lease.bytes = lease.bytes.max(total);
                grown
            }
            None => {
                self.entries.insert(
                    Key::Segment(segment),
                    Entry {
                        member,
                        bytes: total,
                        placing: false,
                    },
                );
                total
            }
        };
        let claim = self
            .entries
            .get_mut(&Key::Claim(claim))
            .expect("claim checked above");
        if grown > claim.bytes {
            tracing::error!(
                grown,
                claimed = claim.bytes,
                "segment lease grew past the bound its WRITE claimed"
            );
        }
        claim.bytes = claim.bytes.saturating_sub(grown);
        Ok(())
    }
}

/// The open segment is leased on a different member than the claim.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MemberMismatch;

/// Room one WRITE holds on one member until its frames are in a segment.
/// Dropping it returns whatever the segment leases did not take.
#[derive(Debug)]
pub(crate) struct Claim {
    ledger: Arc<Mutex<Ledger>>,
    id: u64,
}

impl Claim {
    pub(super) fn new(ledger: Arc<Mutex<Ledger>>, id: u64) -> Self {
        Self { ledger, id }
    }

    /// The member this claim holds room on.
    pub(crate) fn member(&self) -> String {
        self.ledger
            .lock()
            .unwrap()
            .member_of(Key::Claim(self.id))
            .expect("a live claim has a ledger entry")
            .to_owned()
    }

    /// Grow `segment`'s lease to `total` bytes from this claim.
    pub(crate) fn fund(&self, segment: Segid, total: u64) -> Result<(), MemberMismatch> {
        self.ledger.lock().unwrap().fund(self.id, segment, total)
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.ledger.lock().unwrap().remove(Key::Claim(self.id));
    }
}
